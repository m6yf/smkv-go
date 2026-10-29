package smkv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Two wire-level nodes and one discovery endpoint; promotion happens only after
// the old owner actually receives a request, so construction cannot skip it.
func retryFixture(t *testing.T, status int, promote bool) (*ClusterClient, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	listen := func() net.Listener {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}
	old, next, admin := listen(), listen(), listen()
	var moved atomic.Bool
	oldCalls, newCalls := new(atomic.Int32), new(atomic.Int32)
	m := ClusterMap{Cluster: "retry", Epoch: 1, Nodes: []ClusterNode{{ID: "a", Client: old.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:1234"}, {ID: "b", Client: next.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:1235"}}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	go func() {
		for {
			c, e := admin.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				var v any
				if json.NewDecoder(c).Decode(&v) != nil {
					return
				}
				vmap := m
				if moved.Load() {
					vmap.Epoch = 2
					vmap.Partitions = []PartitionOwners{{Primary: "b", Replica: "a"}}
				}
				json.NewEncoder(c).Encode(map[string]any{"ok": true, "result": map[string]any{"map": vmap}})
			}()
		}
	}()
	serve := func(l net.Listener, calls *atomic.Int32, oldNode bool) {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				h := make([]byte, 24)
				if _, e := io.ReadFull(c, h); e != nil {
					return
				}
				_, e := io.CopyN(io.Discard, c, int64(binary.LittleEndian.Uint32(h[8:12]))+int64(binary.LittleEndian.Uint32(h[12:16])))
				if e != nil {
					return
				}
				calls.Add(1)
				s := int(StatusOK)
				if oldNode {
					s = status
					if promote {
						moved.Store(true)
					}
				}
				if s == -1 {
					return
				}
				if s == -2 {
					_, _ = io.Copy(io.Discard, c)
					return
				}
				body := []byte{}
				if s == 0 {
					body = []byte("value")
					if h[4] == opGetRow {
						body, _ = encodeColumns(Columns{"v": "value"}, 1024)
					}
				}
				reply := []byte{'S', 'K', 'R', '1', byte(s), 0, 0, 0, 0, 0, 0, 0}
				binary.LittleEndian.PutUint32(reply[8:], uint32(len(body)))
				c.Write(append(reply, body...))
			}()
		}
	}
	go serve(old, oldCalls, true)
	go serve(next, newCalls, false)
	c, e := NewClusterClient(context.Background(), []string{admin.Addr().String()}, Options{Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c, oldCalls, newCalls
}
func TestClusterReadRetriesTransientFailureOnPromotedOwner(t *testing.T) {
	for _, s := range []int{-1, -2, int(StatusBusy), int(StatusUnavailable), int(StatusTimedOut)} {
		t.Run(fmt.Sprintf("status-%d", s), func(t *testing.T) {
			c, old, next := retryFixture(t, s, true)
			v, e := c.Get(context.Background(), []byte("k"))
			if e != nil || string(v) != "value" || old.Load() != 1 || next.Load() != 1 || c.Map().Epoch != 2 {
				t.Fatalf("value=%q err=%v calls=%d/%d epoch=%d", v, e, old.Load(), next.Load(), c.Map().Epoch)
			}
		})
	}
}
func TestClusterReadDoesNotRetryTerminalFailure(t *testing.T) {
	for _, s := range []Status{StatusNotFound, StatusStorageError, StatusInvalidRequest, StatusCapacity, Status(255)} {
		t.Run(fmt.Sprintf("status-%d", s), func(t *testing.T) {
			c, old, next := retryFixture(t, int(s), true)
			_, e := c.Get(context.Background(), []byte("k"))
			if e == nil || old.Load() != 1 || next.Load() != 0 {
				t.Fatalf("err=%v calls=%d/%d", e, old.Load(), next.Load())
			}
		})
	}
}
func TestClusterReadRetryUsesOneCallerDeadline(t *testing.T) {
	c, old, _ := retryFixture(t, int(StatusUnavailable), false)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	at := time.Now()
	_, e := c.Get(ctx, []byte("k"))
	if !errors.Is(e, context.DeadlineExceeded) || time.Since(at) > 500*time.Millisecond || old.Load() < 2 {
		t.Fatalf("err=%v duration=%v calls=%d", e, time.Since(at), old.Load())
	}
}
func TestClusterCloseCancelsReadRetry(t *testing.T) {
	c, old, _ := retryFixture(t, int(StatusUnavailable), false)
	done := make(chan error, 1)
	go func() { _, e := c.Get(context.Background(), []byte("k")); done <- e }()
	end := time.Now().Add(time.Second)
	for old.Load() == 0 {
		if time.Now().After(end) {
			t.Fatal("request not started")
		}
		time.Sleep(time.Millisecond)
	}
	c.Close()
	select {
	case e := <-done:
		if !errors.Is(e, ErrClosed) {
			t.Fatalf("got %v", e)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("close did not cancel retry")
	}
}

func TestClusterRowReadRecoversButRowMutationDoesNotRetryBusy(t *testing.T) {
	c, old, next := retryFixture(t, -1, true)
	row, e := c.Table("test").Get(context.Background(), []byte("k"))
	if e != nil || row["v"] != "value" || old.Load() != 1 || next.Load() != 1 {
		t.Fatalf("row=%v err=%v calls=%d/%d", row, e, old.Load(), next.Load())
	}
	c, old, next = retryFixture(t, int(StatusBusy), true)
	e = c.Table("test").Update(context.Background(), []byte("k"), Columns{"v": "new"}, nil)
	if !errors.Is(e, ErrBusy) || old.Load() != 1 || next.Load() != 0 {
		t.Fatalf("err=%v calls=%d/%d", e, old.Load(), next.Load())
	}
}
func TestClusterReplicaReadKeepsReplicaRouting(t *testing.T) {
	c, old, next := retryFixture(t, int(StatusUnavailable), false)
	v, e := c.GetReplica(context.Background(), []byte("k"))
	if e != nil || string(v) != "value" || old.Load() != 0 || next.Load() != 1 {
		t.Fatalf("value=%q err=%v calls=%d/%d", v, e, old.Load(), next.Load())
	}
}
func TestClusterCloseCancelsReadWaitingForRefresh(t *testing.T) {
	c, _, _ := retryFixture(t, int(StatusUnavailable), false)
	c.refresh <- struct{}{}
	c.mu.Lock()
	c.refreshNeeded = true
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, e := c.Get(context.Background(), []byte("k")); done <- e }()
	c.Close()
	select {
	case e := <-done:
		if !errors.Is(e, ErrClosed) {
			t.Fatalf("got %v", e)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("refresh wait did not stop")
	}
	<-c.refresh
}
