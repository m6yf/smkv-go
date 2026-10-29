package smkv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestClusterMapValidation(t *testing.T) {
	m := ClusterMap{Cluster: "x", Epoch: 1, Nodes: []ClusterNode{{ID: "a", Client: "localhost:1", Admin: "localhost:2", Replication: "localhost:3"}, {ID: "b", Client: "localhost:4", Admin: "localhost:5", Replication: "localhost:6"}}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	if err := validateMap(m); err != nil {
		t.Fatal(err)
	}
	m.Partitions[0].Replica = "a"
	if validateMap(m) == nil {
		t.Fatal("duplicate owner accepted")
	}
}
func TestClusterDoesNotRetryUnknownMutation(t *testing.T) {
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	m := ClusterMap{Cluster: "x", Epoch: 1, Nodes: []ClusterNode{{ID: "a", Client: data.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:1234"}, {ID: "b", Client: "localhost:1235", Admin: "localhost:1236", Replication: "localhost:1237"}}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	go func() {
		conn, e := admin.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		var v any
		_ = json.NewDecoder(conn).Decode(&v)
		_ = json.NewEncoder(conn).Encode(map[string]any{"ok": true, "result": map[string]any{"map": m}})
	}()
	go func() {
		conn, e := data.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 26)
		_, _ = io.ReadFull(conn, buf)
	}()
	c, err := NewClusterClient(context.Background(), []string{admin.Addr().String()}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Put(context.Background(), []byte("k"), []byte("v")); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("want uncertain outcome: %v", err)
	}
}

func TestClusterRefreshesWrongOwnerWithoutRetryingOldNode(t *testing.T) {
	old, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	next, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	m := ClusterMap{Cluster: "x", Epoch: 1, Nodes: []ClusterNode{{ID: "a", Client: old.Addr().String(), Admin: admin.Addr().String(), Replication: "localhost:1234"}, {ID: "b", Client: next.Addr().String(), Admin: "localhost:1236", Replication: "localhost:1237"}}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	go func() {
		for i := 0; i < 2; i++ {
			conn, e := admin.Accept()
			if e != nil {
				return
			}
			var v any
			_ = json.NewDecoder(conn).Decode(&v)
			if i == 1 {
				m.Epoch = 2
				m.Partitions[0] = PartitionOwners{Primary: "b", Replica: "a"}
			}
			_ = json.NewEncoder(conn).Encode(map[string]any{"ok": true, "result": map[string]any{"map": m}})
			conn.Close()
		}
	}()
	go func() {
		conn, e := old.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 26)
		_, _ = io.ReadFull(conn, buf)
		_, _ = conn.Write([]byte{'S', 'K', 'R', '1', 7, 0, 0, 0, 0, 0, 0, 0})
	}()
	go func() {
		conn, e := next.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 26)
		_, _ = io.ReadFull(conn, buf)
		_, _ = conn.Write([]byte{'S', 'K', 'R', '1', 0, 0, 0, 0, 0, 0, 0, 0})
	}()
	c, err := NewClusterClient(context.Background(), []string{admin.Addr().String()}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if c.Map().Epoch != 2 {
		t.Fatal("map not refreshed")
	}
}

func TestClusterRefreshWaitHonorsDeadline(t *testing.T) {
	opts, _ := (Options{}).defaults()
	c := &ClusterClient{refresh: make(chan struct{}, 1), options: opts, life: context.Background()}
	c.refresh <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := c.Refresh(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("refresh wait exceeded deadline: %v", err)
	}
}

// An uncertain mutation is never retried, but the next call discovers the
// promoted owner even when the original seed continues serving the old epoch.
func TestClusterRecoversNextCallFromUnknownWriteWithStaleSeed(t *testing.T) {
	testClusterRecoversNextCall(t, false)
}
func TestClusterRecoversNextCallFromFencedBusyWithoutRetry(t *testing.T) {
	testClusterRecoversNextCall(t, true)
}
func testClusterRecoversNextCall(t *testing.T, busy bool) {
	old, _ := net.Listen("tcp", "127.0.0.1:0")
	defer old.Close()
	next, _ := net.Listen("tcp", "127.0.0.1:0")
	defer next.Close()
	seed, _ := net.Listen("tcp", "127.0.0.1:0")
	defer seed.Close()
	newer, _ := net.Listen("tcp", "127.0.0.1:0")
	defer newer.Close()
	initial := ClusterMap{Cluster: "recovery", Epoch: 1, Nodes: []ClusterNode{{ID: "a", Client: old.Addr().String(), Admin: seed.Addr().String(), Replication: "localhost:1234"}, {ID: "b", Client: next.Addr().String(), Admin: newer.Addr().String(), Replication: "localhost:1235"}}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	promoted := initial
	promoted.Epoch = 2
	promoted.Partitions = []PartitionOwners{{Primary: "b", Replica: "a"}}
	serve := func(l net.Listener, m ClusterMap) {
		for {
			conn, e := l.Accept()
			if e != nil {
				return
			}
			var v any
			_ = json.NewDecoder(conn).Decode(&v)
			_ = json.NewEncoder(conn).Encode(map[string]any{"ok": true, "result": map[string]any{"map": m}})
			conn.Close()
		}
	}
	go serve(seed, initial)
	go serve(newer, promoted)
	oldCalls := make(chan struct{}, 4)
	go func() {
		for {
			conn, e := old.Accept()
			if e != nil {
				return
			}
			buf := make([]byte, 26)
			_, _ = io.ReadFull(conn, buf)
			oldCalls <- struct{}{}
			if busy {
				_, _ = conn.Write([]byte{'S', 'K', 'R', '1', 2, 0, 0, 0, 0, 0, 0, 0})
			}
			conn.Close()
		}
	}()
	nextCalls := make(chan struct{}, 4)
	go func() {
		conn, e := next.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 26)
		_, _ = io.ReadFull(conn, buf)
		nextCalls <- struct{}{}
		_, _ = conn.Write([]byte{'S', 'K', 'R', '1', 0, 0, 0, 0, 0, 0, 0, 0})
	}()
	c, e := NewClusterClient(context.Background(), []string{seed.Addr().String()}, Options{Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	expected := ErrOutcomeUnknown
	if busy {
		expected = ErrBusy
	}
	if e = c.Put(context.Background(), []byte("k"), []byte("v")); !errors.Is(e, expected) {
		t.Fatalf("want unknown, got %v", e)
	}
	select {
	case <-nextCalls:
		t.Fatal("uncertain mutation was retried")
	default:
	}
	if e = c.Put(context.Background(), []byte("k"), []byte("v")); e != nil {
		t.Fatal(e)
	}
	if c.Map().Epoch != 2 {
		t.Fatal("did not adopt promotion")
	}
	if len(oldCalls) != 1 || len(nextCalls) != 1 {
		t.Fatal("unexpected retry or routing")
	}
}
