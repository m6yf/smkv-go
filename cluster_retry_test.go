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
				// A connection serves requests until it fails, as a server's does.
				for {
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
					if s == 0 && (h[4] == opGet || h[4] == opGetRow) {
						body = []byte("value")
						if h[4] == opGetRow {
							body, _ = encodeColumns(Columns{"v": "value"}, 1024)
						}
					}
					reply := []byte{'S', 'K', 'R', '1', byte(s), 0, 0, 0, 0, 0, 0, 0}
					binary.LittleEndian.PutUint32(reply[8:], uint32(len(body)))
					if _, e := c.Write(append(reply, body...)); e != nil {
						return
					}
				}
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
			// Busy comes from a node that may still own the key: it is asked
			// again before the map is refreshed.
			asked := int32(1)
			if s == int(StatusBusy) {
				asked = busyRefreshAfter
			}
			if e != nil || string(v) != "value" || old.Load() != asked || next.Load() != 1 || c.Map().Epoch != 2 {
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

func TestClusterRowReadRecoversAndRowMutationRetriesBusy(t *testing.T) {
	c, old, next := retryFixture(t, -1, true)
	row, e := c.Table("test").Get(context.Background(), []byte("k"))
	if e != nil || row["v"] != "value" || old.Load() != 1 || next.Load() != 1 {
		t.Fatalf("row=%v err=%v calls=%d/%d", row, e, old.Load(), next.Load())
	}
	// Busy is a definite refusal: the update is sent again, and once Busy
	// has persisted the map is refreshed and the promoted owner takes it.
	c, old, next = retryFixture(t, int(StatusBusy), true)
	e = c.Table("test").Update(context.Background(), []byte("k"), Columns{"v": "new"}, nil)
	if e != nil || old.Load() != busyRefreshAfter || next.Load() != 1 {
		t.Fatalf("err=%v calls=%d/%d", e, old.Load(), next.Load())
	}
}

// A node that stays Busy and still owns the key: the write is refused until
// the budget ends, and the error says so (not applied), not a bare deadline.
func TestClusterMutationRefusedUntilTheBudgetEndsReportsBusy(t *testing.T) {
	c, old, next := retryFixture(t, int(StatusBusy), false)
	started := time.Now()
	e := c.Put(context.Background(), []byte("k"), []byte("v"))
	if !errors.Is(e, ErrBusy) || errors.Is(e, ErrOutcomeUnknown) {
		t.Fatalf("got %v", e)
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("gave up after %v of a 1 s budget", elapsed)
	}
	if old.Load() < 5 || next.Load() != 0 {
		t.Fatalf("calls %d/%d", old.Load(), next.Load())
	}
}

// One seed that answers nothing must not cost every refresh its probe
// timeout: once another seed has answered, the rest get a short grace.
func TestClusterRefreshDoesNotWaitOutASilentSeed(t *testing.T) {
	c, _, _ := retryFixture(t, 0, false)
	silent, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer silent.Close()
	go func() {
		for {
			conn, e := silent.Accept()
			if e != nil {
				return
			}
			defer conn.Close() // accepted, never answered
		}
	}()
	c.mu.Lock()
	c.seeds = append(c.seeds, silent.Addr().String())
	c.mu.Unlock()
	started := time.Now()
	if e = c.Refresh(context.Background()); e != nil {
		t.Fatal(e)
	}
	if elapsed := time.Since(started); elapsed > 4*refreshGrace {
		t.Fatalf("refresh took %v with one silent seed", elapsed)
	}
}

// When no seed answers, a write still goes to the owner the client's map
// names, and the operations behind a failed refresh do not each repeat it.
func TestClusterWritesOnItsMapWhenNoSeedAnswers(t *testing.T) {
	c, old, _ := retryFixture(t, 0, false)
	dead, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := dead.Addr().String()
	dead.Close()
	c.mu.Lock()
	c.seeds = []string{address}
	c.refreshNeeded = true
	c.mu.Unlock()
	started := time.Now()
	for i := 0; i < 20; i++ {
		if e = c.Put(context.Background(), []byte("k"), []byte("v")); e != nil {
			t.Fatalf("write %d failed without being sent: %v", i, e)
		}
	}
	if old.Load() != 20 {
		t.Fatalf("calls %d", old.Load())
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("20 writes took %v behind failed refreshes", elapsed)
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
