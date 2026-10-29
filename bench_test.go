package smkv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"
)

// benchServer answers every request with a fixed 1 KiB value, without
// allocating per request, so allocation counts measure the client.
func benchServer(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = listener.Close() })
	reply := testResponse(StatusOK, make([]byte, 1024))
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var header [requestHeaderSize]byte
				body := make([]byte, 64*1024)
				for {
					if _, err := io.ReadFull(conn, header[:]); err != nil {
						return
					}
					n := int(binary.LittleEndian.Uint32(header[8:12]) + binary.LittleEndian.Uint32(header[12:16]))
					if _, err := io.ReadFull(conn, body[:n]); err != nil {
						return
					}
					if _, err := conn.Write(reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// benchCluster serves a one-partition map whose primary is `data`.
func benchCluster(b *testing.B, data string) string {
	b.Helper()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = admin.Close() })
	m := ClusterMap{Cluster: "bench", Epoch: 1, Nodes: []ClusterNode{
		{ID: "a", Client: data, Admin: admin.Addr().String(), Replication: "127.0.0.1:1"},
		{ID: "b", Client: "127.0.0.1:2", Admin: "127.0.0.1:3", Replication: "127.0.0.1:4"},
	}, Partitions: []PartitionOwners{{Primary: "a", Replica: "b"}}}
	go func() {
		for {
			conn, err := admin.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var v any
				_ = json.NewDecoder(conn).Decode(&v)
				_ = json.NewEncoder(conn).Encode(map[string]any{"ok": true, "result": map[string]any{"map": m}})
			}()
		}
	}()
	return admin.Addr().String()
}

func BenchmarkClientGet(b *testing.B) {
	client, err := NewClient(benchServer(b), Options{PoolSize: 64, Timeout: 3 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	// A context shared by every caller, as a load generator's run context is.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	key := []byte("bench-key-0000000001")
	b.ReportAllocs()
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Get(ctx, key); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkClusterGet(b *testing.B) {
	seed := benchCluster(b, benchServer(b))
	client, err := NewClusterClient(context.Background(), []string{seed}, Options{PoolSize: 64, Timeout: 3 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	key := []byte("bench-key-0000000001")
	b.ReportAllocs()
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Get(ctx, key); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
