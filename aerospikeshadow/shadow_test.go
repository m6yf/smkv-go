package aerospikeshadow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	as8 "github.com/aerospike/aerospike-client-go/v8"
	smkv "github.com/m6yf/smkv-go"
)

// fakeStore is SMKV's rows in memory, with Update refusing a missing row as
// SMKV does.
type fakeStore struct {
	mu     sync.Mutex
	rows   map[string]smkv.Columns
	calls  int
	fail   error         // returned by every call when set
	refuse int           // the next calls refused with ErrBusy
	hold   chan struct{} // when set, every call waits for it to close
}

func newFakeStore() *fakeStore { return &fakeStore{rows: map[string]smkv.Columns{}} }

func (f *fakeStore) enter() error {
	if f.hold != nil {
		<-f.hold
	}
	f.mu.Lock()
	f.calls++
	if f.refuse > 0 {
		f.refuse--
		return smkv.ErrBusy
	}
	return f.fail
}

func (f *fakeStore) Put(_ context.Context, table string, key []byte, columns smkv.Columns) error {
	err := f.enter()
	defer f.mu.Unlock()
	if err == nil {
		row := smkv.Columns{}
		for k, v := range columns {
			row[k] = v
		}
		f.rows[table+"/"+string(key)] = row
	}
	return err
}

func (f *fakeStore) Update(_ context.Context, table string, key []byte, set smkv.Columns, remove []string) error {
	err := f.enter()
	defer f.mu.Unlock()
	if err != nil {
		return err
	}
	row, ok := f.rows[table+"/"+string(key)]
	if !ok {
		return smkv.ErrNotFound
	}
	for k, v := range set {
		row[k] = v
	}
	for _, k := range remove {
		delete(row, k)
	}
	return nil
}

func (f *fakeStore) Delete(_ context.Context, table string, key []byte) error {
	err := f.enter()
	defer f.mu.Unlock()
	if err == nil {
		delete(f.rows, table+"/"+string(key))
	}
	return err
}

func (f *fakeStore) Get(_ context.Context, table string, key []byte, projection ...string) (smkv.Columns, error) {
	err := f.enter()
	defer f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	row, ok := f.rows[table+"/"+string(key)]
	if !ok {
		return nil, smkv.ErrNotFound
	}
	got := smkv.Columns{}
	for k, v := range row {
		got[k] = v
	}
	if len(projection) > 0 {
		got = smkv.Columns{}
		for _, k := range projection {
			if v, ok := row[k]; ok {
				got[k] = v
			}
		}
	}
	return got, nil
}

func (f *fakeStore) row(table, key string) (smkv.Columns, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[table+"/"+key]
	return row, ok
}

type counters struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counters) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[name]++
}

func (c *counters) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}

// testShadow is a connected shadow on a fake store, copying namespace app.
func testShadow(t *testing.T, config Config) (*Shadow, *fakeStore, *counters) {
	t.Helper()
	if config.Recheck == 0 {
		config.Recheck = 50 * time.Millisecond
	}
	c := &counters{n: map[string]int{}}
	config.Namespace = "app"
	config.Count = c.add
	s := newShadow(config)
	t.Cleanup(func() { s.Close() })
	store := newFakeStore()
	s.attach(store)
	return s, store, c
}

// eventually fails the test unless ok holds within two seconds.
func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func userKey(t *testing.T, set, id string) *as.Key {
	t.Helper()
	key, err := as.NewKey("app", set, id)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestShadowWriteCreatesAMissingRowAndKeepsOtherBins(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "lookup", "k1")
	s.write("app", key.SetName(), key.Value(), false, []bin{
		{"payload", as.NewBytesValue([]byte{1, 2})}, {"ip", as.NewValue("10.0.0.1")},
	})
	// As Aerospike's default write does, a write without ip keeps it.
	s.write("app", key.SetName(), key.Value(), false, []bin{{"payload", as.NewBytesValue([]byte{3})}})
	eventually(t, "both writes", func() bool { return c.get("merge") == 2 })
	row, _ := store.row("lookup", "k1")
	if mismatch(true, smkv.Columns{"payload": []byte{3}, "ip": "10.0.0.1"}, true, row) != "" {
		t.Fatalf("row %v", row)
	}
}

func TestShadowReplaceDropsBinsNotWritten(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "user", "g1")
	s.write("app", "user", key.Value(), false, []bin{{"payload", "a"}, {"extra", "x"}})
	s.write("app", "user", key.Value(), true, []bin{{"payload", "b"}})
	eventually(t, "both writes", func() bool { return c.get("merge")+c.get("replace") == 2 })
	if row, _ := store.row("user", "g1"); len(row) != 1 || row["payload"] != "b" {
		t.Fatalf("row %v", row)
	}
}

func TestShadowNilBinRemovesTheColumn(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "id", "g1")
	s.write("app", "id", key.Value(), false, []bin{{"a", "s"}, {"b", "i"}})
	s.write("app", "id", key.Value(), false, []bin{{"b", as.NewNullValue()}})
	eventually(t, "both writes", func() bool { return c.get("merge") == 2 })
	if row, _ := store.row("id", "g1"); len(row) != 1 || row["a"] != "s" {
		t.Fatalf("row %v", row)
	}
}

func TestShadowDeleteOfAMissingRowSucceeds(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "user", "g1")
	s.write("app", "user", key.Value(), false, []bin{{"payload", "a"}})
	s.remove("app", "user", key.Value())
	s.remove("app", "user", userKey(t, "user", "never").Value())
	eventually(t, "the deletes", func() bool { return c.get("delete") == 2 })
	if _, ok := store.row("user", "g1"); ok {
		t.Fatal("the row is still there")
	}
}

func TestShadowKeepsOneKeysWritesInOrder(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 8, Queue: 1024})
	key := userKey(t, "user", "g1")
	for i := 0; i < 500; i++ {
		s.write("app", "user", key.Value(), false, []bin{{"payload", fmt.Sprint(i)}})
		other := userKey(t, "user", fmt.Sprint("other", i))
		s.write("app", "user", other.Value(), false, []bin{{"payload", "x"}})
	}
	eventually(t, "every write", func() bool { return c.get("merge") == 1000 })
	if row, _ := store.row("user", "g1"); row["payload"] != "499" {
		t.Fatalf("the last write was not the last applied: %v", row)
	}
}

func TestShadowDropsCopiesWhenItsQueueIsFull(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 1, Queue: 2})
	store.hold = make(chan struct{})
	start := time.Now()
	for i := 0; i < 10; i++ {
		s.write("app", "user", userKey(t, "user", fmt.Sprint(i)).Value(), false, []bin{{"payload", "x"}})
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("writes waited for SMKV: %v", elapsed)
	}
	// One copy is in the store's call, two wait in the queue.
	if dropped := c.get("dropped"); dropped < 7 {
		t.Fatalf("dropped %d of 10 with room for 3", dropped)
	}
	close(store.hold)
	eventually(t, "the queued copies", func() bool { return c.get("merge")+c.get("dropped") == 10 })
}

func TestShadowSendsNothingBeforeItConnects(t *testing.T) {
	c := &counters{n: map[string]int{}}
	s := newShadow(Config{Namespace: "app", Lanes: 1, Count: c.add})
	defer s.Close()
	s.write("app", "user", userKey(t, "user", "g1").Value(), false, []bin{{"payload", "x"}})
	if c.get("not_connected") != 1 {
		t.Fatalf("counters %v", c.n)
	}
}

func TestShadowSendsNothingOnceClosed(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.write("app", "user", userKey(t, "user", "g1").Value(), false, []bin{{"payload", "x"}})
	s.remove("app", "user", userKey(t, "user", "g1").Value())
	time.Sleep(50 * time.Millisecond)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.calls != 0 || len(c.n) != 0 {
		t.Fatalf("calls %d, counters %v", store.calls, c.n)
	}
}

func TestShadowOffFileStopsCopies(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	path := filepath.Join(t.TempDir(), "smkv-off")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	go s.watchOffFile(path)
	eventually(t, "the switch", s.off.Load)
	s.write("app", "user", userKey(t, "user", "g1").Value(), false, []bin{{"payload", "x"}})
	if s.sampled() {
		t.Fatal("a read was sampled while off")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the switch back", func() bool { return !s.off.Load() })
	s.write("app", "user", userKey(t, "user", "g2").Value(), false, []bin{{"payload", "y"}})
	eventually(t, "the copy after", func() bool { return c.get("merge") == 1 })
	if _, ok := store.row("user", "g1"); ok {
		t.Fatal("a copy was sent while off")
	}
}

func TestShadowPausesAfterRepeatedFailures(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 1, Queue: 256})
	store.fail = smkv.ErrUnavailable
	for i := 0; i < failuresBeforePause+10; i++ {
		s.write("app", "user", userKey(t, "user", fmt.Sprint(i)).Value(), false, []bin{{"payload", "x"}})
	}
	eventually(t, "the pause", func() bool { return c.get("merge_error")+c.get("paused") == failuresBeforePause+10 })
	if c.get("merge_error") != failuresBeforePause || c.get("paused") != 10 {
		t.Fatalf("counters %v", c.n)
	}
}

func TestShadowCountsCopiesSMKVRefuses(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 1})
	store.fail = fmt.Errorf("%w: value too large", smkv.ErrInvalidArgument)
	s.write("app", "user", userKey(t, "user", "g1").Value(), true, []bin{{"payload", "x"}})
	eventually(t, "the refusal", func() bool { return c.get("replace_invalid") == 1 })
	if s.failures.Load() != 0 {
		t.Fatal("a refused copy counted toward the pause")
	}
}

func TestShadowSendsACopySMKVRefusedAgain(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 1})
	store.refuse = 2
	s.write("app", "user", userKey(t, "user", "b1").Value(), false, []bin{{"payload", "x"}})
	s.write("app", "user", userKey(t, "user", "b1").Value(), false, []bin{{"payload", "y"}})
	eventually(t, "the copies", func() bool { return c.get("merge") == 2 })
	if c.get("refused") != 2 || c.get("merge_error") != 0 {
		t.Fatalf("counters %v", c.n)
	}
	if row, _ := store.row("user", "b1"); !reflect.DeepEqual(row, smkv.Columns{"payload": "y"}) {
		t.Fatalf("row %v", row)
	}
}

func TestShadowGivesUpOnARefusedCopyAtItsTimeout(t *testing.T) {
	s, store, c := testShadow(t, Config{Lanes: 1, Timeout: 100 * time.Millisecond})
	store.refuse = 1 << 30
	s.write("app", "user", userKey(t, "user", "b2").Value(), false, []bin{{"payload", "x"}})
	eventually(t, "the failure", func() bool { return c.get("merge_error") == 1 })
	if n := c.get("refused"); n < 2 || n > 5 {
		t.Fatalf("refused %d times in 100 ms", n)
	}
}

func TestShadowAddressesRowsAsTheMigrationDoes(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	oms, _ := as.NewKey("app", "", "1.2.3.4")
	s.write("app", "", oms.Value(), false, []bin{{"payload", "x"}})
	intKey, _ := as.NewKey("app", "n", 7)
	s.write("app", "n", intKey.Value(), false, []bin{{"v", 1}})
	s.write("other", "user", userKey(t, "user", "g1").Value(), false, []bin{{"payload", "x"}})
	s.write("app", "user", nil, false, []bin{{"payload", "x"}})
	s.write("app", "user", userKey(t, "user", "g2").Value(), false, []bin{{"list", []int{1}}})
	eventually(t, "the copies", func() bool { return c.get("merge") == 2 })
	if _, ok := store.row("unset", "1.2.3.4"); !ok {
		t.Fatal("a record without a set did not go to table unset")
	}
	if row, ok := store.row("n", "\x07\x00\x00\x00\x00\x00\x00\x00"); !ok || row["v"] != int64(1) {
		t.Fatalf("integer key or value: %v %v", row, ok)
	}
	if c.get("other_namespace") != 1 || c.get("no_user_key") != 1 || c.get("unsupported") != 1 {
		t.Fatalf("counters %v", c.n)
	}
}

func TestShadowComparesReads(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "user", "g1")
	s.write("app", "user", key.Value(), false, []bin{{"payload", "a"}, {"blob", []byte{1}}})
	eventually(t, "the copy", func() bool { return c.get("merge") == 1 })

	s.compare("app", "user", key.Value(), nil, as.BinMap{"payload": "a", "blob": []byte{1}}, true, nil)
	s.compare("app", "user", key.Value(), []string{"payload"}, as.BinMap{"payload": "a"}, true, nil)
	s.compare("app", "user", userKey(t, "user", "none").Value(), nil, nil, false, nil)
	// A read naming bins compares only those, though Aerospike returned more.
	s.compare("app", "user", key.Value(), []string{"blob"}, as.BinMap{"payload": "a", "blob": []byte{1}}, true, nil)
	eventually(t, "the matches", func() bool { return c.get("match") == 4 })

	s.compare("app", "user", key.Value(), nil, as.BinMap{"payload": "b", "blob": []byte{1}}, true, nil)
	s.compare("app", "user", userKey(t, "user", "absent").Value(), nil, as.BinMap{"payload": "a"}, true, nil)
	s.compare("app", "user", key.Value(), nil, nil, false, nil)
	eventually(t, "the mismatches", func() bool {
		return c.get("mismatch_value") == 1 && c.get("mismatch_missing") == 1 && c.get("mismatch_extra") == 1
	})
	// The copy's Update found no row and Put it; each mismatch was read twice.
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.calls != 2+4+3*2 {
		t.Fatalf("%d calls", store.calls)
	}
}

func TestShadowSecondLookReadsAerospikeAgain(t *testing.T) {
	s, _, c := testShadow(t, Config{})
	key := userKey(t, "user", "r1")
	// SMKV holds a write Aerospike applied after the sampled read.
	s.write("app", "user", key.Value(), true, []bin{{"payload", "b"}})
	eventually(t, "the copy", func() bool { return c.get("replace") == 1 })
	again := func() (map[string]interface{}, bool, error) {
		return map[string]interface{}{"payload": "b"}, true, nil
	}
	s.compare("app", "user", key.Value(), nil, as.BinMap{"payload": "a"}, true, again)
	eventually(t, "the second look", func() bool { return c.get("converged") == 1 })
	failed := func() (map[string]interface{}, bool, error) { return nil, false, errors.New("timeout") }
	s.compare("app", "user", key.Value(), nil, as.BinMap{"payload": "a"}, true, failed)
	eventually(t, "the failed second look", func() bool { return c.get("compare_error") == 1 })
	if c.get("mismatch_value") != 0 || s.failures.Load() != 0 {
		t.Fatalf("counters %v, failures %d", c.n, s.failures.Load())
	}
}

func TestShadowCopiesWholeAKeyTwoCallsWroteAtOnce(t *testing.T) {
	s, store, c := testShadow(t, Config{})
	key := userKey(t, "user", "w1")
	first := s.begin("app", "user", key.Value())
	second := s.begin("app", "user", key.Value())
	// Aerospike applied "new" last, but its call returned first.
	s.write("app", "user", key.Value(), false, []bin{{"payload", "new"}})
	if s.end(second) {
		t.Fatal("a repair before the last call ended")
	}
	s.write("app", "user", key.Value(), false, []bin{{"payload", "old"}, {"ip", "x"}})
	if !s.end(first) {
		t.Fatal("no repair after two calls at once")
	}
	s.repair(first, func() (map[string]interface{}, bool, error) {
		return map[string]interface{}{"payload": "new"}, true, nil
	})
	eventually(t, "the repair", func() bool { return c.get("repair") == 1 })
	if row, _ := store.row("user", "w1"); !reflect.DeepEqual(row, smkv.Columns{"payload": "new"}) {
		t.Fatalf("row %v", row)
	}
	// A record Aerospike no longer holds is deleted.
	first, second = s.begin("app", "user", key.Value()), s.begin("app", "user", key.Value())
	s.end(first)
	s.end(second)
	s.repair(second, func() (map[string]interface{}, bool, error) { return nil, false, nil })
	eventually(t, "the second repair", func() bool { return c.get("repair") == 2 })
	if _, ok := store.row("user", "w1"); ok {
		t.Fatal("the row outlived Aerospike's record")
	}
	// One call at a time needs no repair.
	if alone := s.begin("app", "user", key.Value()); s.end(alone) {
		t.Fatal("a repair after one call")
	}
}

func TestShadowCountsAMismatchItsQueuedCopyFixesAsConverged(t *testing.T) {
	s, store, c := testShadow(t, Config{Recheck: 200 * time.Millisecond})
	key := userKey(t, "user", "g1")
	s.compare("app", "user", key.Value(), nil, as.BinMap{"payload": "a"}, true, nil)
	eventually(t, "the first look", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.calls == 1
	})
	s.write("app", "user", key.Value(), false, []bin{{"payload", "a"}})
	eventually(t, "the second look", func() bool { return c.get("converged") == 1 })
	if c.get("mismatch_missing") != 0 {
		t.Fatalf("counters %v", c.n)
	}
}

func TestShadowConvertsBinValuesOfBothClients(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want any
	}{
		{"s", "s"}, {as.NewValue("s"), "s"}, {as8.NewValue("s"), "s"},
		{[]byte{1}, []byte{1}}, {as.NewBytesValue([]byte{1}), []byte{1}}, {as8.NewBytesValue([]byte{1}), []byte{1}},
		{7, int64(7)}, {as.NewValue(7), int64(7)}, {as8.NewValue(int64(7)), int64(7)},
		{1.5, 1.5}, {true, true}, {nil, nil}, {as.NewNullValue(), nil}, {as8.NewNullValue(), nil},
	} {
		got, ok := columnValue(tc.in)
		if !ok || !sameValue(got, tc.want) {
			t.Errorf("columnValue(%#v) = %#v, %v; want %#v", tc.in, got, ok, tc.want)
		}
	}
	for _, unsupported := range []any{[]int{1}, map[string]int{}, as.NewListValue([]interface{}{1}), uint64(1 << 63)} {
		if _, ok := columnValue(unsupported); ok {
			t.Errorf("columnValue(%#v) converted", unsupported)
		}
	}
}

func TestShadowIsOffWithoutSeeds(t *testing.T) {
	s, err := New(Config{Namespace: "app"})
	if s != nil || err != nil {
		t.Fatalf("New without seeds: %v, %v", s, err)
	}
	s.write("app", "user", "g1", false, []bin{{"payload", "x"}})
	s.remove("app", "user", "g1")
	if s.sampled() || s.Close() != nil {
		t.Fatal("a nil shadow did something")
	}
	if _, err := New(Config{Seeds: []string{"127.0.0.1:1"}}); err == nil {
		t.Fatal("New accepted a shadow without a namespace")
	}
	if _, err := New(Config{Seeds: []string{"127.0.0.1:1"}, Namespace: "app", ReadSample: 2}); err == nil {
		t.Fatal("New accepted a read sample above 1")
	}
}

func TestShadowWritePolicies(t *testing.T) {
	update := as.NewWritePolicy(0, 0)
	replace := as.NewWritePolicy(0, 0)
	replace.RecordExistsAction = as.REPLACE
	update8 := as8.NewWritePolicy(0, 0)
	create8 := as8.NewWritePolicy(0, 0)
	create8.RecordExistsAction = as8.CREATE_ONLY
	if replacesV4(nil) || replacesV4(update) || !replacesV4(replace) || replacesV8(nil) || replacesV8(update8) || !replacesV8(create8) {
		t.Fatal("record-exists actions mapped wrongly")
	}
	if !notFoundV4(errors.Join(errors.New("context"), keyNotFoundV4())) {
		t.Fatal("a v4 key-not-found error was not recognised")
	}
}

func keyNotFoundV4() error { return types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR) }
