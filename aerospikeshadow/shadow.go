// Package aerospikeshadow runs SMKV beside Aerospike as a shadow. The
// application keeps its Aerospike client, wrapped with WrapV4 or WrapV8: every
// write Aerospike accepts is also sent to SMKV, and a sample of reads is
// compared with SMKV's copy. Aerospike stays the only source of answers.
// Copies are asynchronous, a full queue drops them rather than wait, and
// SMKV's errors are only counted.
//
// Rows are addressed as SMKV's migration tool converts an Aerospike export, so
// a shadow can start from a bulk load: Config.Namespace maps to the SMKV
// database Config.Options.Database ("default" when empty), each set to the
// table of the same name (records without a set to Config.UnsetTable), and a
// key to a string's UTF-8 bytes, a blob's bytes or an integer's 8 bytes little
// endian.
//
// Writes keep Aerospike's meaning. UPDATE, the default, sets the bins written
// and creates a missing row; REPLACE, REPLACE_ONLY and CREATE_ONLY replace the
// row; a nil bin removes its column; a delete deletes the row. A write
// Aerospike refuses is not copied.
//
// Config.Count receives an event name for each copy and comparison:
//
//   - merge, replace, delete: a copy SMKV applied; with _error, one it failed,
//     and with _invalid, one past its limits. refused: SMKV refused a copy
//     before applying it (Busy), and the copy goes again within its Timeout.
//   - repair: a record copied whole from Aerospike, after calls that wrote its
//     key at the same time ended; their copies may have reached SMKV in
//     another order than Aerospike applied them. repair_error: Aerospike or
//     SMKV failed it.
//   - match: a sampled read SMKV agreed with. converged: one it agreed with on
//     the second look, which reads Aerospike again too, after its copy
//     arrived. mismatch_missing (no row),
//     mismatch_extra (a row Aerospike did not have), mismatch_value: one it
//     still disagreed with. compare_error: SMKV, or on the second look
//     Aerospike, could not be read.
//   - dropped (the queue was full), not_connected, paused (after repeated
//     failures), no_user_key (a key without its value, from a scan of records
//     stored without keys), unsupported (a list, map or other bin SMKV has no
//     column for), other_namespace.
package aerospikeshadow

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	smkv "github.com/m6yf/smkv-go"
)

// Config configures a Shadow. Zero fields select the defaults noted.
type Config struct {
	// Seeds are SMKV management addresses (host:port). Without any, New
	// returns a nil Shadow, which copies nothing.
	Seeds []string
	// Namespace is the Aerospike namespace copied; keys in others are skipped.
	Namespace string
	// Options configure the SMKV client: its Database, TLS and credentials.
	// PoolSize and Timeout default to Lanes and Timeout.
	Options smkv.Options
	// UnsetTable is the table of records stored without a set; "unset".
	UnsetTable string
	// ReadSample is the fraction of reads compared, from 0 (none) to 1.
	ReadSample float64
	// Lanes is the number of concurrent SMKV requests; 32. One key's copies
	// always take the same lane, so they reach SMKV in the order Aerospike
	// accepted them.
	Lanes int
	// Queue is the number of copies waiting per lane before new ones are
	// dropped; 1024.
	Queue int
	// Timeout is each SMKV request's budget; 1 s.
	Timeout time.Duration
	// Recheck is how long a mismatch waits for its second look, in case its
	// copy was still queued; 2 s.
	Recheck time.Duration
	// OffFile, when set, stops all SMKV traffic while that file exists. It is
	// checked every second, so the shadow stops without a restart.
	OffFile string
	// Count receives each event's name (see the package documentation).
	Count func(event string)
	// Logger receives connection changes, pauses and at most one mismatch
	// every 10 s, with the key hashed. Nil is slog.Default().
	Logger *slog.Logger
}

const (
	// After this many SMKV failures in a row, copies are dropped for pauseFor.
	failuresBeforePause = 50
	pauseFor            = 10 * time.Second
	logMismatchEvery    = 10 * time.Second
)

type op uint8

const (
	opMerge   op = iota // Aerospike's default write: set these bins, creating the record if needed
	opReplace           // the record becomes these bins
	opDelete
	opCompare
	opRepair // the record as Aerospike now holds it
)

var opNames = [...]string{"merge", "replace", "delete", "compare", "repair"}

// reread reads a record from Aerospike again: its bins, or found false.
type reread func() (bins map[string]interface{}, found bool, err error)

type job struct {
	op      op
	table   string
	key     []byte
	columns smkv.Columns // merge, replace: the bins written; compare: the bins Aerospike returned
	remove  []string     // merge: bins written as nil, which Aerospike removes
	found   bool         // compare: Aerospike had the record
	project []string     // compare: the bins read, all if empty
	recheck bool         // compare: the second look after a mismatch
	reread  reread       // compare: the read, again; repair: the whole record
}

type bin struct {
	name  string
	value any
}

// store is the part of SMKV the shadow uses; tests replace it.
type store interface {
	Put(ctx context.Context, table string, key []byte, columns smkv.Columns) error
	Update(ctx context.Context, table string, key []byte, set smkv.Columns, remove []string) error
	Delete(ctx context.Context, table string, key []byte) error
	Get(ctx context.Context, table string, key []byte, projection ...string) (smkv.Columns, error)
}

type clusterStore struct{ client *smkv.ClusterClient }

func (s clusterStore) Put(ctx context.Context, table string, key []byte, columns smkv.Columns) error {
	return s.client.Table(table).Put(ctx, key, columns)
}

func (s clusterStore) Update(ctx context.Context, table string, key []byte, set smkv.Columns, remove []string) error {
	return s.client.Table(table).Update(ctx, key, set, remove)
}

func (s clusterStore) Delete(ctx context.Context, table string, key []byte) error {
	return s.client.Table(table).Delete(ctx, key)
}

func (s clusterStore) Get(ctx context.Context, table string, key []byte, projection ...string) (smkv.Columns, error) {
	return s.client.Table(table).Get(ctx, key, projection...)
}

// Shadow copies writes to SMKV and compares reads. A nil *Shadow copies
// nothing, so wrappers built with one pass every call straight through.
type Shadow struct {
	config    Config
	log       *slog.Logger
	store     store // set once, before connected
	connected atomic.Bool
	off       atomic.Bool
	lanes     []chan job
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	client    *smkv.ClusterClient // closed by Close
	failures  atomic.Int64
	pausedTo  atomic.Int64 // unix nanoseconds
	loggedAt  atomic.Int64 // the last mismatch logged, unix nanoseconds
	writing   []writing    // per lane
}

// writing counts the calls writing each key of a lane.
type writing struct {
	mu   sync.Mutex
	keys map[string]*calls
}

type calls struct {
	n         int
	contended bool // two were in Aerospike at once
}

// pending is one call's write of a row, from begin to end.
type pending struct {
	table string
	key   []byte
	lane  int
	id    string
}

// New starts a shadow. It connects to SMKV in the background, retrying until
// it can, and drops copies until then. Without Seeds it returns nil, nil.
func New(config Config) (*Shadow, error) {
	if len(config.Seeds) == 0 {
		return nil, nil
	}
	if config.Namespace == "" {
		return nil, errors.New("aerospikeshadow: Namespace is required")
	}
	if config.ReadSample < 0 || config.ReadSample > 1 {
		return nil, errors.New("aerospikeshadow: ReadSample must be from 0 to 1")
	}
	s := newShadow(config)
	options := s.config.Options
	if options.PoolSize == 0 {
		options.PoolSize = s.config.Lanes
	}
	if options.Timeout == 0 {
		options.Timeout = s.config.Timeout
	}
	go s.connect(config.Seeds, options)
	if config.OffFile != "" {
		go s.watchOffFile(config.OffFile)
	}
	return s, nil
}

func newShadow(config Config) *Shadow {
	if config.UnsetTable == "" {
		config.UnsetTable = "unset"
	}
	if config.Lanes <= 0 {
		config.Lanes = 32
	}
	if config.Queue <= 0 {
		config.Queue = 1024
	}
	if config.Timeout <= 0 {
		config.Timeout = time.Second
	}
	if config.Recheck <= 0 {
		config.Recheck = 2 * time.Second
	}
	s := &Shadow{config: config, log: config.Logger, lanes: make([]chan job, config.Lanes), done: make(chan struct{}),
		writing: make([]writing, config.Lanes)}
	if s.log == nil {
		s.log = slog.Default()
	}
	for i := range s.lanes {
		s.lanes[i] = make(chan job, config.Queue)
		go s.run(s.lanes[i])
	}
	return s
}

// Close stops the shadow: nothing more is sent, and its SMKV client closes.
func (s *Shadow) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.client != nil {
			err = s.client.Close()
		}
	})
	return err
}

func (s *Shadow) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *Shadow) count(event string) {
	if s.config.Count != nil {
		s.config.Count(event)
	}
}

func (s *Shadow) connect(seeds []string, options smkv.Options) {
	for delay := time.Second; ; delay = min(2*delay, time.Minute) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := smkv.NewClusterClient(ctx, seeds, options)
		cancel()
		if err == nil {
			s.mu.Lock()
			if s.closed() {
				s.mu.Unlock()
				client.Close()
				return
			}
			s.client = client
			s.mu.Unlock()
			s.attach(clusterStore{client})
			s.log.Info("aerospikeshadow: connected to SMKV")
			return
		}
		s.log.Warn("aerospikeshadow: cannot reach SMKV, retrying", "error", err)
		select {
		case <-s.done:
			return
		case <-time.After(delay):
		}
	}
}

// attach starts sending copies to st.
func (s *Shadow) attach(st store) {
	s.store = st
	s.connected.Store(true)
}

func (s *Shadow) watchOffFile(path string) {
	for {
		_, err := os.Stat(path)
		if off := err == nil; off != s.off.Swap(off) {
			s.log.Warn("aerospikeshadow: switched", "off", off, "file", path)
		}
		select {
		case <-s.done:
			return
		case <-time.After(time.Second):
		}
	}
}

// sampled reports whether to compare this read.
func (s *Shadow) sampled() bool {
	return s != nil && s.connected.Load() && !s.off.Load() && rand.Float64() < s.config.ReadSample
}

// begin records a call writing a key, before it goes to Aerospike. Calls
// that write one key at once return in any order, so their copies can reach
// SMKV in another order than Aerospike applied them; when the last ends, the
// record is copied whole from Aerospike.
func (s *Shadow) begin(namespace, set string, userKey any) *pending {
	if s == nil || s.off.Load() || !s.connected.Load() {
		return nil
	}
	table, key, ok := s.locate(namespace, set, userKey)
	if !ok {
		return nil
	}
	p := &pending{table: table, key: key, lane: s.lane(table, key), id: table + "\x00" + string(key)}
	w := &s.writing[p.lane]
	w.mu.Lock()
	if w.keys == nil {
		w.keys = map[string]*calls{}
	}
	if c := w.keys[p.id]; c != nil {
		c.n++
		c.contended = true
	} else {
		w.keys[p.id] = &calls{n: 1}
	}
	w.mu.Unlock()
	return p
}

// end records that p's call returned, after its copy was queued. True means
// the record must be copied whole: call repair.
func (s *Shadow) end(p *pending) bool {
	if p == nil {
		return false
	}
	w := &s.writing[p.lane]
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.keys[p.id]
	c.n--
	if c.n > 0 {
		return false
	}
	delete(w.keys, p.id)
	return c.contended
}

// repair copies p's record whole, as whole reads it from Aerospike.
func (s *Shadow) repair(p *pending, whole reread) {
	s.enqueue(job{op: opRepair, table: p.table, key: p.key, reread: whole})
}

// write copies a write Aerospike accepted. replace is true when the record
// now holds only these bins.
func (s *Shadow) write(namespace, set string, userKey any, replace bool, bins []bin) {
	if s == nil || s.off.Load() {
		return
	}
	table, key, ok := s.row(namespace, set, userKey)
	if !ok {
		return
	}
	j := job{op: opMerge, table: table, key: key, columns: make(smkv.Columns, len(bins))}
	if replace {
		j.op = opReplace
	}
	for _, b := range bins {
		value, ok := columnValue(b.value)
		switch {
		case !ok:
			s.count("unsupported")
			return
		case value != nil:
			j.columns[b.name] = value
		case !replace:
			j.remove = append(j.remove, b.name)
		}
	}
	s.enqueue(j)
}

// remove copies a delete Aerospike accepted.
func (s *Shadow) remove(namespace, set string, userKey any) {
	if s == nil || s.off.Load() {
		return
	}
	if table, key, ok := s.row(namespace, set, userKey); ok {
		s.enqueue(job{op: opDelete, table: table, key: key})
	}
}

// compare queues a comparison of SMKV's row with what Aerospike returned for
// a read of project (all bins if empty). found is false when Aerospike had no
// record. again repeats the read, for the second look after a mismatch.
func (s *Shadow) compare(namespace, set string, userKey any, project []string, bins map[string]interface{}, found bool, again reread) {
	table, key, ok := s.row(namespace, set, userKey)
	if !ok {
		return
	}
	j := job{op: opCompare, table: table, key: key, found: found, project: project, reread: again}
	if found {
		if j.columns, ok = columns(bins, project); !ok {
			s.count("unsupported")
			return
		}
	}
	s.enqueue(j)
}

// columns is a record's bins as SMKV columns, only project's if any; ok is
// false if a bin has no column type.
func columns(bins map[string]interface{}, project []string) (smkv.Columns, bool) {
	c := make(smkv.Columns, len(bins))
	for name, v := range bins {
		if len(project) > 0 && !slices.Contains(project, name) {
			// The v4 client gets every bin from Aerospike 8, whichever it names.
			continue
		}
		value, ok := columnValue(v)
		if !ok || value == nil {
			return nil, false
		}
		c[name] = value
	}
	return c, true
}

// row addresses a key's SMKV row, counting why not when it cannot.
func (s *Shadow) row(namespace, set string, userKey any) (string, []byte, bool) {
	if namespace != s.config.Namespace {
		s.count("other_namespace")
		return "", nil, false
	}
	table, key, ok := s.locate(namespace, set, userKey)
	if !ok {
		s.count("no_user_key")
	}
	return table, key, ok
}

// locate is row without counting.
func (s *Shadow) locate(namespace, set string, userKey any) (string, []byte, bool) {
	key, ok := rowKey(userKey)
	if namespace != s.config.Namespace || !ok {
		return "", nil, false
	}
	if set == "" {
		set = s.config.UnsetTable
	}
	return set, key, true
}

// rowKey is the row key of an Aerospike user key: a string's UTF-8 bytes, a
// blob's bytes, an integer's 8 bytes little endian.
func rowKey(userKey any) ([]byte, bool) {
	if v, ok := userKey.(interface{ GetObject() interface{} }); ok {
		userKey = v.GetObject()
	}
	switch k := userKey.(type) {
	case string:
		return []byte(k), k != ""
	case []byte:
		return bytes.Clone(k), len(k) > 0
	case int:
		return binary.LittleEndian.AppendUint64(nil, uint64(int64(k))), true
	case int64:
		return binary.LittleEndian.AppendUint64(nil, uint64(k)), true
	}
	return nil, false
}

// columnValue is a bin value as an SMKV column value, or nil for a nil bin.
// ok is false for lists, maps and the other types SMKV has no column for.
func columnValue(v any) (any, bool) {
	if o, ok := v.(interface{ GetObject() interface{} }); ok {
		v = o.GetObject()
	}
	switch x := v.(type) {
	case nil:
		return nil, true
	case string, int64, float64, bool:
		return x, true
	case []byte:
		return bytes.Clone(x), true
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint:
		return int64(x), x <= math.MaxInt64
	case uint64:
		return int64(x), x <= math.MaxInt64
	case float32:
		return float64(x), true
	}
	return nil, false
}

func (s *Shadow) enqueue(j job) {
	if s.closed() {
		return
	}
	if !s.connected.Load() {
		s.count("not_connected")
		return
	}
	select {
	case s.lanes[s.lane(j.table, j.key)] <- j:
	default:
		s.count("dropped")
	}
}

// lane is the FNV-1a hash of the row's table and key, modulo the lanes.
func (s *Shadow) lane(table string, key []byte) int {
	h := uint32(2166136261)
	for i := 0; i < len(table); i++ {
		h = (h ^ uint32(table[i])) * 16777619
	}
	h *= 16777619
	for _, b := range key {
		h = (h ^ uint32(b)) * 16777619
	}
	return int(h % uint32(len(s.lanes)))
}

func (s *Shadow) run(lane chan job) {
	for {
		var j job
		select {
		case <-s.done:
			return
		case j = <-lane:
		}
		if s.off.Load() {
			continue
		}
		if time.Now().UnixNano() < s.pausedTo.Load() {
			s.count("paused")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
		err := s.apply(ctx, j)
		cancel()
		if err == nil || errors.Is(err, smkv.ErrInvalidArgument) {
			s.failures.Store(0)
		} else if s.failures.Add(1) >= failuresBeforePause {
			s.failures.Store(0)
			s.pausedTo.Store(time.Now().Add(pauseFor).UnixNano())
			s.log.Warn("aerospikeshadow: SMKV keeps failing, pausing copies", "error", err, "for", pauseFor)
		}
	}
}

func (s *Shadow) apply(ctx context.Context, j job) error {
	name := opNames[j.op]
	switch j.op {
	case opCompare:
		return s.check(ctx, j)
	case opRepair:
		bins, found, err := j.reread()
		switch {
		case err != nil:
			s.count("repair_error")
			return nil // Aerospike's failure, not SMKV's
		case !found:
			j.op = opDelete
		default:
			var ok bool
			if j.columns, ok = columns(bins, nil); !ok {
				s.count("unsupported")
				return nil
			}
			j.op = opReplace
		}
	}
	err := s.copy(ctx, j)
	// A definite refusal (Busy) applied nothing, so the copy goes again while
	// its budget lasts. The lane waits, which keeps the key's writes in order.
	for delay := 10 * time.Millisecond; refused(err); delay *= 2 {
		s.count("refused")
		if !pause(ctx, delay) {
			break
		}
		err = s.copy(ctx, j)
	}
	switch {
	case err == nil:
		s.count(name)
	case errors.Is(err, smkv.ErrInvalidArgument):
		s.count(name + "_invalid")
	default:
		s.count(name + "_error")
	}
	return err
}

func refused(err error) bool {
	return errors.Is(err, smkv.ErrBusy) && !errors.Is(err, smkv.ErrOutcomeUnknown)
}

// pause waits for d, or returns false if ctx ends first.
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Shadow) copy(ctx context.Context, j job) error {
	var err error
	switch j.op {
	case opReplace:
		err = s.store.Put(ctx, j.table, j.key, j.columns)
	case opMerge:
		err = s.store.Update(ctx, j.table, j.key, j.columns, j.remove)
		if errors.Is(err, smkv.ErrNotFound) {
			// Aerospike creates a missing record; SMKV's Update needs the row.
			err = nil
			if len(j.columns) > 0 {
				err = s.store.Put(ctx, j.table, j.key, j.columns)
			}
		}
	case opDelete:
		err = s.store.Delete(ctx, j.table, j.key)
		if errors.Is(err, smkv.ErrNotFound) {
			err = nil
		}
	}
	return err
}

func (s *Shadow) check(ctx context.Context, j job) error {
	if j.recheck && j.reread != nil {
		// Aerospike may have changed too, by a write whose copy is queued
		// after this look.
		bins, found, err := j.reread()
		if err != nil {
			s.count("compare_error")
			return nil // Aerospike's failure, not SMKV's
		}
		j.found, j.columns = found, nil
		if found {
			var ok bool
			if j.columns, ok = columns(bins, j.project); !ok {
				s.count("unsupported")
				return nil
			}
		}
	}
	got, err := s.store.Get(ctx, j.table, j.key, j.project...)
	found := err == nil
	if errors.Is(err, smkv.ErrNotFound) {
		err = nil
	}
	if err != nil {
		s.count("compare_error")
		return err
	}
	verdict := mismatch(j.found, j.columns, found, got)
	switch {
	case verdict == "" && j.recheck:
		s.count("converged")
	case verdict == "":
		s.count("match")
	case !j.recheck:
		j.recheck = true
		time.AfterFunc(s.config.Recheck, func() { s.enqueue(j) })
	default:
		s.count("mismatch_" + verdict)
		s.logMismatch(j, verdict)
	}
	return nil
}

// mismatch is "" when SMKV holds what Aerospike returned; else "missing" (no
// row), "extra" (a row Aerospike did not have) or "value".
func mismatch(wantFound bool, want smkv.Columns, gotFound bool, got smkv.Columns) string {
	switch {
	case wantFound && !gotFound:
		return "missing"
	case gotFound && !wantFound:
		return "extra"
	case len(want) != len(got):
		return "value"
	}
	for name, w := range want {
		g, ok := got[name]
		if !ok || !sameValue(w, g) {
			return "value"
		}
	}
	return ""
}

func sameValue(a, b any) bool {
	if x, ok := a.([]byte); ok {
		y, ok := b.([]byte)
		return ok && bytes.Equal(x, y)
	}
	return a == b
}

// logMismatch logs at most one mismatch per logMismatchEvery, with the key
// hashed; the events carry the rate.
func (s *Shadow) logMismatch(j job, verdict string) {
	now := time.Now().UnixNano()
	last := s.loggedAt.Load()
	if now-last < int64(logMismatchEvery) || !s.loggedAt.CompareAndSwap(last, now) {
		return
	}
	h := uint64(14695981039346656037)
	for _, b := range j.key {
		h = (h ^ uint64(b)) * 1099511628211
	}
	s.log.Warn("aerospikeshadow: SMKV differs from Aerospike",
		"table", j.table, "key_fnv64", strconv.FormatUint(h, 16), "mismatch", verdict)
}
