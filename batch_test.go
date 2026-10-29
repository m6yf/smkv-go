package smkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// parseBatchList decodes a batch get key list as the server does.
func parseBatchList(t *testing.T, list []byte, database bool) (string, [][]byte) {
	t.Helper()
	name := ""
	if database {
		n := int(binary.LittleEndian.Uint16(list))
		name, list = string(list[2:2+n]), list[2+n:]
	}
	count := int(binary.LittleEndian.Uint32(list))
	list = list[4:]
	keys := make([][]byte, count)
	for i := range keys {
		n := int(binary.LittleEndian.Uint32(list))
		keys[i], list = list[4:4+n], list[4+n:]
	}
	if len(list) != 0 {
		t.Errorf("trailing bytes in batch list")
	}
	return name, keys
}

type testEntry struct {
	status Status
	value  []byte
}

func batchReply(entries []testEntry) []byte {
	body := binary.LittleEndian.AppendUint32(nil, uint32(len(entries)))
	for _, e := range entries {
		body = append(body, byte(e.status))
		body = binary.LittleEndian.AppendUint32(body, uint32(len(e.value)))
		body = append(body, e.value...)
	}
	return testResponse(StatusOK, body)
}

func TestBatchGetWireFormatAndOrderedResults(t *testing.T) {
	requests := make(chan testRequest, 1)
	client, _ := pipeClient(t, Options{Database: "tenant"}, func(conn net.Conn) {
		request, err := readTestRequest(conn)
		if err != nil {
			return
		}
		requests <- request
		_, _ = conn.Write(batchReply([]testEntry{{StatusOK, []byte("one")}, {StatusNotFound, nil}, {StatusOK, nil}}))
	})
	values, err := client.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if string(values[0]) != "one" || values[1] != nil || values[2] == nil || len(values[2]) != 0 {
		t.Fatalf("values %q", values)
	}
	request := <-requests
	header := request.header
	if header[4] != opBatchGet || header[5] != 8 || binary.LittleEndian.Uint32(header[12:16]) != 0 ||
		binary.LittleEndian.Uint64(header[16:24]) != ^uint64(0) {
		t.Fatalf("header % x", header)
	}
	name, keys := parseBatchList(t, request.key, true)
	if name != "tenant" || len(keys) != 3 || string(keys[0]) != "a" || string(keys[2]) != "c" {
		t.Fatalf("list %q %q", name, keys)
	}
}

func TestBatchGetSplitsByKeyLimitAndResendsDeferredKeys(t *testing.T) {
	var batches atomic.Int32
	var deferred atomic.Bool
	client, _ := pipeClient(t, Options{MaxBatchKeys: 2}, func(conn net.Conn) {
		for {
			request, err := readTestRequest(conn)
			if err != nil {
				return
			}
			batches.Add(1)
			_, keys := parseBatchList(t, request.key, false)
			if len(keys) > 2 {
				t.Errorf("batch of %d keys", len(keys))
			}
			entries := make([]testEntry, len(keys))
			for i, key := range keys {
				entries[i] = testEntry{StatusOK, append([]byte("v"), key...)}
				// The first time key "b" is asked, it is past the reply cap.
				if string(key) == "b" && deferred.CompareAndSwap(false, true) {
					entries[i] = testEntry{status: StatusBusy}
				}
			}
			_, _ = conn.Write(batchReply(entries))
		}
	})
	values, err := client.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"a", "b", "c", "d", "e"} {
		if string(values[i]) != "v"+key {
			t.Fatalf("value %d = %q", i, values[i])
		}
	}
	// [a b] [b c] [d e]: the deferred key leads the next request.
	if batches.Load() != 3 {
		t.Fatalf("%d batches", batches.Load())
	}
}

func TestBatchGetFailsOnAPerKeyError(t *testing.T) {
	client, _ := pipeClient(t, Options{}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err == nil {
			_, _ = conn.Write(batchReply([]testEntry{{StatusOK, []byte("x")}, {status: StatusWrongOwner}}))
		}
	})
	_, err := client.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b")})
	if !errors.Is(err, ErrWrongOwner) {
		t.Fatalf("error %v", err)
	}
}

func TestBatchGetFallsBackToSingleGetsOnAnOlderServer(t *testing.T) {
	var refused atomic.Int32
	client, _ := pipeClient(t, Options{}, func(conn net.Conn) {
		for {
			request, err := readTestRequest(conn)
			if err != nil {
				return
			}
			switch {
			case request.header[4] == opBatchGet:
				// An older server refuses the operation and closes.
				refused.Add(1)
				_, _ = conn.Write(testResponse(StatusInvalidRequest, []byte("unknown operation")))
				return
			case string(request.key) == "missing":
				_, _ = conn.Write(testResponse(StatusNotFound, nil))
			default:
				_, _ = conn.Write(testResponse(StatusOK, request.key))
			}
		}
	})
	for range 2 {
		values, err := client.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("missing"), []byte("bb")})
		if err != nil || string(values[0]) != "a" || values[1] != nil || string(values[2]) != "bb" {
			t.Fatalf("values %q error %v", values, err)
		}
	}
	if refused.Load() != 1 {
		t.Fatalf("batches refused %d times", refused.Load())
	}
}

func TestBatchGetDoesNotFallBackOnOtherInvalidRequests(t *testing.T) {
	client, _ := pipeClient(t, Options{}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err == nil {
			_, _ = conn.Write(testResponse(StatusInvalidRequest, []byte("batch key count outside configured limits")))
		}
	})
	_, err := client.BatchGet(context.Background(), [][]byte{[]byte("a")})
	if !errors.Is(err, ErrInvalidRequest) || client.noBatches.Load() {
		t.Fatalf("error %v, fell back %v", err, client.noBatches.Load())
	}
}

func TestBatchGetRejectsMalformedReplies(t *testing.T) {
	for _, body := range [][]byte{
		{},
		{2, 0, 0, 0, 0, 1, 0, 0, 0, 'x'},      // count mismatch
		{1, 0, 0, 0, 1, 1, 0, 0, 0, 'x'},      // a value on a miss
		{1, 0, 0, 0, 0, 5, 0, 0, 0, 'x'},      // truncated value
		{1, 0, 0, 0, 0, 1, 0, 0, 0, 'x', 'y'}, // trailing bytes
		{1, 0, 0, 0, 42, 0, 0, 0, 0},          // unknown status
	} {
		if _, err := decodeBatch(body, 1); !errors.Is(err, ErrProtocol) {
			t.Fatalf("body % x: %v", body, err)
		}
	}
}

func TestBatchGetValidatesKeysBeforeSending(t *testing.T) {
	client, dials := pipeClient(t, Options{MaxKeyBytes: 4}, func(net.Conn) {})
	for _, keys := range [][][]byte{{[]byte("a"), {}}, {[]byte("12345")}} {
		if _, err := client.BatchGet(context.Background(), keys); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("keys %q: %v", keys, err)
		}
	}
	if dials.Load() != 0 {
		t.Fatal("invalid batch used a connection")
	}
	values, err := client.BatchGet(context.Background(), nil)
	if err != nil || len(values) != 0 {
		t.Fatalf("empty batch %q %v", values, err)
	}
}

// Two nodes that each answer only their own partitions' keys (the value is
// the key), and a discovery endpoint whose map can swap their ownership.
func TestClusterBatchGetGroupsByNodeMergesInOrderAndFollowsMoves(t *testing.T) {
	listen := func() net.Listener {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}
	a, b, admin := listen(), listen(), listen()
	var epoch atomic.Uint64
	epoch.Store(1)
	const partitions = 8
	owner := func(e uint64, partition uint32) string {
		if (partition%2 == 0) == (e == 1) {
			return "a"
		}
		return "b"
	}
	mapAt := func(e uint64) ClusterMap {
		m := ClusterMap{Cluster: "batch", Epoch: e, Nodes: []ClusterNode{
			{ID: "a", Client: a.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:1"},
			{ID: "b", Client: b.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:2"}}}
		for p := uint32(0); p < partitions; p++ {
			primary := owner(e, p)
			replica := map[string]string{"a": "b", "b": "a"}[primary]
			m.Partitions = append(m.Partitions, PartitionOwners{Primary: primary, Replica: replica})
		}
		return m
	}
	go func() {
		for {
			c, e := admin.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				var v any
				if json.NewDecoder(c).Decode(&v) == nil {
					json.NewEncoder(c).Encode(map[string]any{"ok": true, "result": map[string]any{"map": mapAt(epoch.Load())}})
				}
			}()
		}
	}()
	var batches [2]atomic.Int32
	serve := func(l net.Listener, id string, calls *atomic.Int32) {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					h := make([]byte, 24)
					if _, e := io.ReadFull(c, h); e != nil {
						return
					}
					list := make([]byte, binary.LittleEndian.Uint32(h[8:12]))
					if _, e := io.ReadFull(c, list); e != nil {
						return
					}
					calls.Add(1)
					_, keys := parseBatchList(t, list, false)
					entries := make([]testEntry, len(keys))
					for i, key := range keys {
						entries[i] = testEntry{status: StatusWrongOwner}
						if owner(epoch.Load(), crc32Partition(key, partitions)) == id {
							entries[i] = testEntry{StatusOK, key}
							if key[0] == 'm' {
								entries[i] = testEntry{status: StatusNotFound}
							}
						}
					}
					if _, e := c.Write(batchReply(entries)); e != nil {
						return
					}
				}
			}()
		}
	}
	go serve(a, "a", &batches[0])
	go serve(b, "b", &batches[1])
	client, err := NewClusterClient(context.Background(), []string{admin.Addr().String()}, Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	var keys [][]byte
	for i := 0; i < 40; i++ {
		prefix := byte('k')
		if i%5 == 0 {
			prefix = 'm'
		}
		keys = append(keys, []byte{prefix, byte(i)})
	}
	check := func() {
		t.Helper()
		values, err := client.BatchGet(context.Background(), keys)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			want := key
			if key[0] == 'm' {
				want = nil
			}
			if !bytes.Equal(values[i], want) || (want != nil && values[i] == nil) {
				t.Fatalf("key %d: %q", i, values[i])
			}
		}
	}
	check()
	if batches[0].Load() != 1 || batches[1].Load() != 1 {
		t.Fatalf("one batch per node expected: %d %d", batches[0].Load(), batches[1].Load())
	}
	// Ownership swaps: every key first comes back WrongOwner, then the
	// client refreshes and asks the new owners.
	epoch.Store(2)
	check()
	if got := client.Map().Epoch; got != 2 {
		t.Fatalf("map epoch %d", got)
	}
}

func crc32Partition(key []byte, partitions uint32) uint32 {
	return crc32.ChecksumIEEE(key) % partitions
}

func TestVersionedRequestsWireFormatAndConflicts(t *testing.T) {
	requests := make(chan testRequest, 4)
	replies := [][]byte{
		testResponse(StatusOK, append(binary.LittleEndian.AppendUint64(nil, 7<<16|2), "value"...)),
		testResponse(StatusOK, binary.LittleEndian.AppendUint64(nil, 7<<16|3)),
		testResponse(StatusConflict, nil),
		testResponse(StatusOK, binary.LittleEndian.AppendUint64(nil, 0)),
	}
	// One connection, so the scripted replies stay in order.
	client, _ := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		for _, reply := range replies {
			request, err := readTestRequest(conn)
			if err != nil {
				return
			}
			requests <- request
			if _, err := conn.Write(reply); err != nil {
				return
			}
		}
	})
	ctx := context.Background()
	value, version, err := client.GetVersioned(ctx, []byte("k"))
	if err != nil || string(value) != "value" || version != 7<<16|2 {
		t.Fatalf("get %q %d %v", value, version, err)
	}
	if r := <-requests; r.header[4] != opGet || r.header[5] != 16 || len(r.value) != 0 {
		t.Fatalf("get header % x", r.header)
	}
	next, err := client.PutIf(ctx, []byte("k"), []byte("new"), version)
	if err != nil || next != 7<<16|3 {
		t.Fatalf("put %d %v", next, err)
	}
	r := <-requests
	if r.header[5] != 16|32 || binary.LittleEndian.Uint64(r.value) != version || string(r.value[8:]) != "new" {
		t.Fatalf("put header % x value % x", r.header, r.value)
	}
	_, err = client.PutIf(ctx, []byte("k"), []byte("lost"), version)
	if !errors.Is(err, ErrConflict) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("conflict %v", err)
	}
	<-requests
	if err := client.DeleteIf(ctx, []byte("k"), next); err != nil {
		t.Fatal(err)
	}
	if r := <-requests; r.header[4] != opDelete || r.header[5] != 16|32 || binary.LittleEndian.Uint64(r.value) != next {
		t.Fatalf("delete header % x value % x", r.header, r.value)
	}
	if err := client.DeleteIf(ctx, []byte("k"), 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("delete at version 0: %v", err)
	}
}
