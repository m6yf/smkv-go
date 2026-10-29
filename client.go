package smkv

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type pooledConn struct {
	conn     net.Conn
	lastUsed time.Time
	// Reused by every request on this connection, so a call allocates little
	// beyond the value it returns.
	out  []byte
	head [responseHeaderSize]byte
	// Interrupts in-flight I/O when the caller's context ends; signals once done.
	interrupt   func()
	interrupted chan struct{}
}

// Requests up to this size are copied into one write; larger values are
// written in place with writev.
const writeCopyLimit = 64 * 1024

func newPooledConn(conn net.Conn) *pooledConn {
	pc := &pooledConn{conn: conn, interrupted: make(chan struct{}, 1)}
	pc.interrupt = func() {
		_ = pc.conn.SetDeadline(time.Now())
		pc.interrupted <- struct{}{}
	}
	return pc
}

func (pc *pooledConn) write(op byte, key, value []byte, options writeOptions) (int64, error) {
	header := requestHeader(op, key, value, options)
	if len(value) > writeCopyLimit {
		// Only this copy of the header escapes, and only for large values.
		large := header
		buffers := net.Buffers{large[:], key, value}
		return buffers.WriteTo(pc.conn)
	}
	pc.out = append(append(append(pc.out[:0], header[:]...), key...), value...)
	written, err := pc.conn.Write(pc.out)
	return int64(written), err
}

// request is one operation: its name for errors, its wire operation, and its
// arguments.
type request struct {
	name       string
	op         byte
	key, value []byte
	options    writeOptions
}

// Client is safe for concurrent use. Each leased connection executes one complete
// request/response at a time. Calls across connections have no ordering guarantee;
// await a Put before issuing a dependent Get. Do not copy a Client after creation.
type Client struct {
	address     string
	options     Options
	slots       chan *pooledConn
	life        context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	connections map[net.Conn]struct{}
	dial        func(context.Context, string, string) (net.Conn, error)
	// The server refused batch get once (an older server): use single gets.
	noBatches atomic.Bool
}

// NewClient validates configuration but connects lazily on the first operation.
// The address must be host:port (IPv6 addresses use brackets).
func NewClient(address string, options Options) (*Client, error) {
	options, err := options.defaults()
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return nil, fmt.Errorf("%w: address must be host:port", ErrInvalidArgument)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return nil, fmt.Errorf("%w: invalid port", ErrInvalidArgument)
	}
	life, cancel := context.WithCancel(context.Background())
	client := &Client{
		address: address, options: options, slots: make(chan *pooledConn, options.PoolSize),
		life: life, cancel: cancel, connections: make(map[net.Conn]struct{}),
		dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return options.Security.dial(ctx, address)
		},
	}
	for i := 0; i < options.PoolSize; i++ {
		client.slots <- nil
	}
	return client, nil
}

// Get returns ErrNotFound for a missing or expired key. Empty values are successful.
// The returned bytes belong to the caller.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.execute(ctx, request{name: "get", op: opGet, key: key})
}

// Put stores opaque bytes. Omit WithTTL for no expiration. Key and value slices
// must not be modified until the call returns; the SDK does not retain them.
func (c *Client) Put(ctx context.Context, key, value []byte, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err != nil {
		return &OpError{Op: "put", Err: err}
	}
	_, err = c.execute(ctx, request{name: "put", op: opPut, key: key, value: value, options: parsed})
	return err
}

// Delete is idempotent: deleting an absent key succeeds. Sync and replica acknowledgment options apply; TTL is rejected.
func (c *Client) Delete(ctx context.Context, key []byte, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err == nil && parsed.ttlSet {
		err = fmt.Errorf("%w: TTL is only valid for Put", ErrInvalidArgument)
	}
	if err != nil {
		return &OpError{Op: "delete", Err: err}
	}
	_, err = c.execute(ctx, request{name: "delete", op: opDelete, key: key, options: parsed})
	return err
}

// GetVersioned returns the value of key and its version
// (docs/proposals/generation-cas.md), or ErrNotFound. A version is opaque:
// compare it only for equality. It changes with every write of the record and
// every ownership change of its partition. Device-engine servers only.
func (c *Client) GetVersioned(ctx context.Context, key []byte) ([]byte, uint64, error) {
	body, err := c.execute(ctx, request{name: "get", op: opGet, key: key, options: writeOptions{report: true}})
	return splitVersion("get", body, err)
}

// PutIf writes only while key is at version expected, or only if it is
// absent when expected is 0, as Aerospike's generation check. It returns the
// new version; a mismatch fails with ErrConflict and writes nothing.
func (c *Client) PutIf(ctx context.Context, key, value []byte, expected uint64, options ...WriteOption) (uint64, error) {
	parsed, err := parseWriteOptions(options)
	if err != nil {
		return 0, &OpError{Op: "put", Err: err}
	}
	parsed.report, parsed.expect, parsed.expected = true, true, expected
	body, err := c.execute(ctx, request{name: "put", op: opPut, key: key, value: value, options: parsed})
	_, version, err := splitVersion("put", body, err)
	return version, err
}

// DeleteIf deletes only while key is at version expected; a mismatch fails
// with ErrConflict. TTL is rejected.
func (c *Client) DeleteIf(ctx context.Context, key []byte, expected uint64, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err == nil && (parsed.ttlSet || expected == 0) {
		err = fmt.Errorf("%w: TTL, or an expected version of 0", ErrInvalidArgument)
	}
	if err != nil {
		return &OpError{Op: "delete", Err: err}
	}
	parsed.report, parsed.expect, parsed.expected = true, true, expected
	_, err = c.execute(ctx, request{name: "delete", op: opDelete, key: key, options: parsed})
	return err
}

// splitVersion separates a versioned reply into its value and version.
func splitVersion(op string, body []byte, err error) ([]byte, uint64, error) {
	if err != nil {
		return nil, 0, err
	}
	if len(body) < 8 {
		return nil, 0, &OpError{Op: op, Err: fmt.Errorf("%w: versioned reply too short", ErrProtocol)}
	}
	return body[8:], binary.LittleEndian.Uint64(body), nil
}

// Close aborts active sockets, cancels dialing and pool waits, and closes idle
// connections. It is idempotent. Aborted mutations can have unknown outcomes.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cancel()
	connections := c.connections
	c.connections = make(map[net.Conn]struct{})
	c.mu.Unlock()
	for conn := range connections {
		_ = conn.Close()
	}
	return nil
}

func (c *Client) execute(ctx context.Context, r request) ([]byte, error) {
	if ctx == nil {
		return nil, &OpError{Op: r.name, Err: fmt.Errorf("%w: nil context", ErrInvalidArgument)}
	}
	return c.executeBy(ctx, c.deadline(ctx), r)
}

// deadline ends an operation: the configured timeout from now, or the
// context's deadline if that is sooner.
func (c *Client) deadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(c.options.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		return d
	}
	return deadline
}

// executeBy runs one request that must finish by `deadline`. The deadline is
// set on the socket rather than carried by a derived context, and the
// caller's context is watched only if it can be canceled, so a call allocates
// little beyond the value it returns.
func (c *Client) executeBy(ctx context.Context, deadline time.Time, r request) ([]byte, error) {
	fail := func(err error, uncertain bool) ([]byte, error) {
		return nil, &OpError{Op: r.name, Err: err, OutcomeUnknown: uncertain}
	}
	key, value, op, options := r.key, r.value, r.op, r.options
	if ctx == nil || len(key) == 0 || len(key) > c.options.MaxKeyBytes || len(value) > c.options.MaxValueBytes {
		return fail(fmt.Errorf("%w: nil context, empty key, or size limit exceeded", ErrInvalidArgument), false)
	}
	key = databaseKey(c.options.Database, key)
	if len(key) > c.options.MaxKeyBytes {
		return fail(ErrInvalidArgument, false)
	}
	options.database = c.options.Database != "default" && c.options.Database != ""
	limit := c.options.MaxValueBytes
	if options.report {
		// A versioned reply leads with the 8-byte version.
		if op == opGet {
			limit += 8
		} else {
			limit = 8
		}
	}
	if options.expect {
		value = append(binary.LittleEndian.AppendUint64(make([]byte, 0, 8+len(value)), options.expected), value...)
	}
	result, written, err := c.roundTrip(ctx, deadline, op, key, value, options, limit)
	mutating := op != opGet && op != opGetRow && op != opScan
	uncertain := mutating && written && err != nil
	if err == nil {
		switch result.status {
		case StatusOK:
		case StatusNotFound:
			err = ErrNotFound
		default:
			err = &ServerError{Status: result.status, Message: string(result.body)}
			uncertain = mutating && (result.status == StatusStorageError || result.status == StatusUnavailable || result.status == StatusTimedOut)
		}
	}
	if err != nil {
		return fail(err, uncertain)
	}
	return result.body, nil
}

// roundTrip sends one request on a pooled connection and reads its reply,
// whose OK body may hold up to `limit` bytes. It reports whether any of the
// request was written.
func (c *Client) roundTrip(ctx context.Context, deadline time.Time, op byte, key, value []byte, options writeOptions, limit int) (response, bool, error) {
	var result response
	pc, err := c.acquire(ctx, deadline)
	if err != nil {
		return result, false, c.contextError(ctx, deadline, err)
	}
	if err = pc.conn.SetDeadline(deadline); err != nil {
		c.release(pc, true)
		return result, false, c.contextError(ctx, deadline, err)
	}
	// Wait for a started cancellation callback before resetting/releasing a socket.
	// Otherwise it could set a deadline on the next caller's request.
	var stop func() bool
	if ctx.Done() != nil {
		stop = context.AfterFunc(ctx, pc.interrupt)
	}
	written, err := pc.write(op, key, value, options)
	if err == nil {
		result, err = readResponse(pc.conn, &pc.head, op, limit, options.report)
	}
	if stop != nil && !stop() {
		<-pc.interrupted
	}
	reusable := err == nil && ctx.Err() == nil && result.status != StatusUnauthorized &&
		result.status != StatusInvalidRequest && result.status != StatusTimedOut
	if err != nil {
		err = c.contextError(ctx, deadline, err)
	}
	if reusable {
		if clearErr := pc.conn.SetDeadline(time.Time{}); clearErr != nil {
			reusable = false
		}
	}
	c.release(pc, !reusable)
	return result, written > 0, err
}

// BatchGet returns the values of keys in order, nil for a miss, in one request
// per MaxBatchKeys keys (as Aerospike's batch read returns a nil record for a
// missing key). A stored empty value is a non-nil empty slice. Keys the server
// defers (Busy, past its reply cap) are resent; any other per-key failure
// fails the call with its status. Against a server without batch get, it
// falls back to single gets. The returned bytes belong to the caller.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if ctx == nil {
		return nil, &OpError{Op: "batch get", Err: fmt.Errorf("%w: nil context", ErrInvalidArgument)}
	}
	entries, err := c.batchEntries(ctx, c.deadline(ctx), keys)
	if err != nil {
		return nil, &OpError{Op: "batch get", Err: err}
	}
	values := make([][]byte, len(keys))
	for i, entry := range entries {
		switch entry.status {
		case StatusOK:
			values[i] = entry.value
		case StatusNotFound:
		default:
			return nil, &OpError{Op: "batch get", Err: &ServerError{Status: entry.status}}
		}
	}
	return values, nil
}

// batchEntries answers keys (raw keys of the client's database) with per-key
// statuses and values, resending deferred keys until `deadline`.
func (c *Client) batchEntries(ctx context.Context, deadline time.Time, keys [][]byte) ([]batchEntry, error) {
	for _, key := range keys {
		if len(key) == 0 || len(databaseKey(c.options.Database, key)) > c.options.MaxKeyBytes {
			return nil, fmt.Errorf("%w: empty key or size limit exceeded", ErrInvalidArgument)
		}
	}
	entries := make([]batchEntry, len(keys))
	pending := make([]int, len(keys))
	for i := range pending {
		pending[i] = i
	}
	delay := time.Millisecond
	for len(pending) > 0 {
		if !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		chunk := pending[:min(len(pending), c.options.MaxBatchKeys)]
		var replies []batchEntry
		var err error
		if !c.noBatches.Load() {
			replies, err = c.batchRequest(ctx, deadline, keys, chunk)
		}
		if err == nil && replies == nil {
			c.noBatches.Store(true)
			replies, err = c.singleGets(ctx, deadline, keys, chunk)
		}
		if err != nil {
			return nil, err
		}
		var deferred []int
		for j, i := range chunk {
			if replies[j].status == StatusBusy {
				deferred = append(deferred, i)
			}
			entries[i] = replies[j]
		}
		if len(deferred) == len(chunk) {
			// No progress: the server is busy, not just at its reply cap.
			timer := time.NewTimer(min(delay, max(time.Until(deadline), 0)))
			select {
			case <-ctx.Done():
			case <-c.life.Done():
			case <-timer.C:
			}
			timer.Stop()
			if err := c.contextError(ctx, deadline, nil); err != nil {
				return nil, err
			}
			delay = min(2*delay, 50*time.Millisecond)
		}
		pending = append(deferred, pending[len(chunk):]...)
	}
	return entries, nil
}

// batchRequest sends one batch get for `chunk`. It returns nil entries and no
// error if the server lacks batch get.
func (c *Client) batchRequest(ctx context.Context, deadline time.Time, keys [][]byte, chunk []int) ([]batchEntry, error) {
	list := batchList(c.options.Database, keys, chunk)
	options := writeOptions{database: c.options.Database != "default" && c.options.Database != ""}
	result, _, err := c.roundTrip(ctx, deadline, opBatchGet, list, nil, options, c.options.MaxBatchResponseBytes)
	if err != nil {
		return nil, err
	}
	switch {
	case result.status == StatusOK:
		return decodeBatch(result.body, len(chunk))
	case result.status == StatusInvalidRequest && string(result.body) == "unknown operation":
		// An older server: it does not know the operation.
		return nil, nil
	default:
		return nil, &ServerError{Status: result.status, Message: string(result.body)}
	}
}

// singleGets answers `chunk` one get at a time, for servers without batch get.
func (c *Client) singleGets(ctx context.Context, deadline time.Time, keys [][]byte, chunk []int) ([]batchEntry, error) {
	replies := make([]batchEntry, len(chunk))
	for j, i := range chunk {
		value, err := c.executeBy(ctx, deadline, request{name: "get", op: opGet, key: keys[i]})
		var server *ServerError
		switch {
		case err == nil:
			replies[j] = batchEntry{StatusOK, value}
		case errors.Is(err, ErrNotFound):
			replies[j] = batchEntry{status: StatusNotFound}
		case errors.As(err, &server):
			replies[j] = batchEntry{status: server.Status}
		default:
			return nil, err
		}
	}
	return replies, nil
}

func (c *Client) contextError(ctx context.Context, deadline time.Time, fallback error) error {
	if c.life.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The operation's deadline is the socket's: report its expiry as the
	// context error a derived context would have returned.
	if errors.Is(fallback, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if timeout, ok := fallback.(net.Error); ok && timeout.Timeout() && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return fallback
}

func (c *Client) acquire(ctx context.Context, deadline time.Time) (*pooledConn, error) {
	var pc *pooledConn
	select {
	case pc = <-c.slots:
	default:
		// The pool is busy: wait, bounded by the operation's deadline.
		wait := time.NewTimer(time.Until(deadline))
		defer wait.Stop()
		select {
		case <-c.life.Done():
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait.C:
			return nil, context.DeadlineExceeded
		case pc = <-c.slots:
		}
	}
	if ctx.Err() != nil || c.life.Err() != nil || !time.Now().Before(deadline) {
		c.release(pc, false)
		if ctx.Err() == nil && c.life.Err() == nil {
			return nil, context.DeadlineExceeded
		}
		return nil, c.contextError(ctx, deadline, context.Canceled)
	}
	if pc != nil && time.Since(pc.lastUsed) >= c.options.IdleTimeout {
		c.discard(pc)
		pc = nil
	}
	if pc != nil {
		return pc, nil
	}
	// Dialing is rare; it may use a derived context. Close cancels it.
	dialCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	stopClose := context.AfterFunc(c.life, cancel)
	defer stopClose()
	conn, err := c.dial(dialCtx, "tcp", c.address)
	if err != nil {
		c.release(nil, false)
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		c.release(nil, false)
		return nil, ErrClosed
	}
	c.connections[conn] = struct{}{}
	c.mu.Unlock()
	return newPooledConn(conn), nil
}

func (c *Client) discard(pc *pooledConn) {
	c.mu.Lock()
	delete(c.connections, pc.conn)
	c.mu.Unlock()
	_ = pc.conn.Close()
}

func (c *Client) release(pc *pooledConn, broken bool) {
	if pc != nil {
		if broken || c.life.Err() != nil {
			c.discard(pc)
			pc = nil
		} else {
			pc.lastUsed = time.Now()
		}
	}
	select {
	case <-c.life.Done():
	case c.slots <- pc:
	}
}
