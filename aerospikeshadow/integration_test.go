//go:build integration

package aerospikeshadow

// These run both wrappers against a disposable Aerospike and SMKV cluster:
//
//	AEROSPIKE_ADDR=127.0.0.1:3000 SMKV_SEEDS=127.0.0.1:7381 go test -race -tags=integration ./...
//
// AEROSPIKE_NAMESPACE selects the namespace written ("test" by default). Every
// key starts with a prefix unique to the run.

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	as8 "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	smkv "github.com/m6yf/smkv-go"
)

type cluster struct {
	host      string
	port      int
	namespace string
	seeds     []string
	smkv      *smkv.ClusterClient
	prefix    string
}

func clusterFromEnv(t *testing.T) *cluster {
	t.Helper()
	host, port, err := net.SplitHostPort(os.Getenv("AEROSPIKE_ADDR"))
	if err != nil || os.Getenv("SMKV_SEEDS") == "" {
		t.Skip("needs AEROSPIKE_ADDR and SMKV_SEEDS")
	}
	c := &cluster{host: host, namespace: os.Getenv("AEROSPIKE_NAMESPACE"), seeds: strings.Split(os.Getenv("SMKV_SEEDS"), ",")}
	c.port, _ = strconv.Atoi(port)
	if c.namespace == "" {
		c.namespace = "test"
	}
	c.smkv, err = smkv.NewClusterClient(context.Background(), c.seeds, smkv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.smkv.Close() })
	c.prefix = "aerospikeshadow-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-"
	return c
}

// shadow is a connected shadow comparing every read.
func (c *cluster) shadow(t *testing.T) (*Shadow, *counters) {
	t.Helper()
	n := &counters{n: map[string]int{}}
	s, err := New(Config{Seeds: c.seeds, Namespace: c.namespace, ReadSample: 1, Count: n.add})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	eventually(t, "the connection to SMKV", s.connected.Load)
	return s, n
}

// sameRow waits for SMKV's row to hold what read returns from Aerospike.
func (c *cluster) sameRow(t *testing.T, table string, key []byte, read func() (map[string]interface{}, bool)) {
	t.Helper()
	bins, found := read()
	want := smkv.Columns{}
	for name, v := range bins {
		want[name], _ = columnValue(v)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got, err := c.smkv.Table(table).Get(context.Background(), key)
		if err != nil && !errors.Is(err, smkv.ErrNotFound) {
			t.Fatal(err)
		}
		if mismatch(found, want, err == nil, got) == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%q: SMKV holds %v, Aerospike %v (found %v)", table, key, got, want, found)
		}
	}
}

func cleanCounters(t *testing.T, n *counters) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	for name, count := range n.n {
		if strings.HasPrefix(name, "mismatch") || strings.HasSuffix(name, "_error") || strings.HasSuffix(name, "_invalid") ||
			name == "dropped" || name == "unsupported" || name == "no_user_key" {
			t.Errorf("%s: %d", name, count)
		}
	}
	t.Logf("events %v", n.n)
}

func TestV4AgainstAerospikeAndSMKV(t *testing.T) {
	c := clusterFromEnv(t)
	s, n := c.shadow(t)
	raw, err := as.NewClientWithPolicyAndHost(as.NewClientPolicy(), as.NewHost(c.host, c.port))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	client := WrapV4(raw, s)
	same := func(key *as.Key) {
		t.Helper()
		table, rowKey, _ := s.row(key.Namespace(), key.SetName(), key.Value())
		c.sameRow(t, table, rowKey, func() (map[string]interface{}, bool) {
			record, err := raw.Get(nil, key)
			if notFoundV4(err) {
				return nil, false
			}
			if err != nil {
				t.Fatal(err)
			}
			return record.Bins, true
		})
	}

	// UPDATE sets the bins written and keeps the others.
	key, _ := as.NewKey(c.namespace, "lookup", c.prefix+"l1")
	if err := client.PutBins(nil, key, as.NewBin("payload", []byte{1, 2}), as.NewBin("ip", "10.0.0.1")); err != nil {
		t.Fatal(err)
	}
	if err := client.PutBins(nil, key, as.NewBin("payload", []byte{3})); err != nil {
		t.Fatal(err)
	}
	same(key)
	// A nil bin removes it.
	if err := client.Put(nil, key, as.BinMap{"ip": nil}); err != nil {
		t.Fatal(err)
	}
	same(key)
	// REPLACE keeps only the bins written.
	replace := as.NewWritePolicy(0, 0)
	replace.RecordExistsAction = as.REPLACE
	if err := client.Put(replace, key, as.BinMap{"n": 7, "f": 1.5, "s": "x"}); err != nil {
		t.Fatal(err)
	}
	same(key)
	// A write Aerospike refuses returns its error and is not copied.
	create := as.NewWritePolicy(0, 0)
	create.RecordExistsAction = as.CREATE_ONLY
	if err := client.Put(create, key, as.BinMap{"n": 8}); err == nil {
		t.Fatal("Aerospike accepted CREATE_ONLY on an existing record")
	}
	same(key)
	if _, err := client.Get(nil, key, "n"); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Delete(nil, key); err != nil || !existed {
		t.Fatalf("delete %v %v", existed, err)
	}
	same(key)
	if _, err := client.Get(nil, key); !notFoundV4(err) {
		t.Fatalf("a miss changed: %v", err)
	}

	// A record without a set, and an integer key.
	unset, _ := as.NewKey(c.namespace, "", c.prefix+"u1")
	if err := client.Put(nil, unset, as.BinMap{"payload": "x"}); err != nil {
		t.Fatal(err)
	}
	same(unset)
	number, _ := as.NewKey(c.namespace, "numbers", time.Now().UnixNano())
	if err := client.Put(nil, number, as.BinMap{"v": "x"}); err != nil {
		t.Fatal(err)
	}
	same(number)
	if _, err := client.Get(nil, number); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the comparisons", func() bool { return n.get("match") >= 3 })
	cleanCounters(t, n)
}

func TestV8AgainstAerospikeAndSMKV(t *testing.T) {
	c := clusterFromEnv(t)
	s, n := c.shadow(t)
	raw, err := as8.NewClientWithPolicyAndHost(as8.NewClientPolicy(), as8.NewHost(c.host, c.port))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	client := WrapV8(raw, s)
	same := func(key *as8.Key) {
		t.Helper()
		table, rowKey, _ := s.row(key.Namespace(), key.SetName(), key.Value())
		c.sameRow(t, table, rowKey, func() (map[string]interface{}, bool) {
			record, err := raw.Get(nil, key)
			if err != nil && err.Matches(types.KEY_NOT_FOUND_ERROR) {
				return nil, false
			}
			if err != nil {
				t.Fatal(err)
			}
			return record.Bins, true
		})
	}

	key, _ := as8.NewKey(c.namespace, "lookup", c.prefix+"l1")
	if err := client.PutBins(nil, key, as8.NewBin("a", 0), as8.NewBin("b", "x")); err != nil {
		t.Fatal(err)
	}
	if err := client.Put(nil, key, as8.BinMap{"a": 1}); err != nil {
		t.Fatal(err)
	}
	same(key)
	replace := as8.NewWritePolicy(0, 0)
	replace.RecordExistsAction = as8.REPLACE
	if err := client.PutBins(replace, key, as8.NewBin("c", []byte{9})); err != nil {
		t.Fatal(err)
	}
	same(key)
	create := as8.NewWritePolicy(0, 0)
	create.RecordExistsAction = as8.CREATE_ONLY
	if err := client.Put(create, key, as8.BinMap{"c": 2}); err == nil || !err.Matches(types.KEY_EXISTS_ERROR) {
		t.Fatalf("Aerospike's refusal did not come back: %v", err)
	}
	same(key)

	missing, _ := as8.NewKey(c.namespace, "lookup", c.prefix+"none")
	if _, err := client.Get(nil, key, "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchGet(nil, []*as8.Key{key, missing}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Delete(nil, key); err != nil {
		t.Fatal(err)
	}
	same(key)

	eventually(t, "the comparisons", func() bool { return n.get("match") >= 3 })
	cleanCounters(t, n)
}

func TestWritesWithSMKVDown(t *testing.T) {
	c := clusterFromEnv(t)
	n := &counters{n: map[string]int{}}
	s, err := New(Config{Seeds: []string{"127.0.0.1:1"}, Namespace: c.namespace, ReadSample: 1, Count: n.add})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, err := as8.NewClientWithPolicyAndHost(as8.NewClientPolicy(), as8.NewHost(c.host, c.port))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	client := WrapV8(raw, s)
	start := time.Now()
	for i := 0; i < 200; i++ {
		key, _ := as8.NewKey(c.namespace, "down", c.prefix+strconv.Itoa(i))
		if err := client.Put(nil, key, as8.BinMap{"v": i}); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Get(nil, key); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("200 writes and reads took %v with SMKV down", elapsed)
	}
	if n.get("not_connected") != 200 {
		t.Fatalf("events %v", n.n)
	}
	t.Logf("200 writes and reads in %v", time.Since(start))
}
