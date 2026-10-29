package smkv

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
)

// ClusterMap is the versioned CRC32 partition map served by cluster admin ports.
type ClusterMap struct {
	Cluster    string            `json:"cluster"`
	Epoch      uint64            `json:"epoch"`
	Nodes      []ClusterNode     `json:"nodes"`
	Partitions []PartitionOwners `json:"partitions"`
}
type ClusterNode struct {
	ID          string `json:"id"`
	Client      string `json:"client"`
	Replication string `json:"replication"`
	Admin       string `json:"admin"`
}
type PartitionOwners struct {
	Primary string `json:"primary"`
	Replica string `json:"replica"`
}

// ClusterClient routes directly to partition owners. Seeds are private admin
// addresses. Reads retry transient failures within one operation budget.
// Mutations retry only explicit WrongOwner responses.
// Mutations with unknown outcomes are returned to the caller without retry.
type ClusterClient struct {
	mu            sync.RWMutex
	refresh       chan struct{}
	seeds         []string
	options       Options
	topology      ClusterMap
	clients       map[string]*Client
	closed        bool
	refreshNeeded bool
	life          context.Context
	cancel        context.CancelFunc
}

func NewClusterClient(ctx context.Context, seeds []string, options Options) (*ClusterClient, error) {
	options, err := options.defaults()
	if err != nil {
		return nil, err
	}
	if ctx == nil || len(seeds) == 0 {
		return nil, ErrInvalidArgument
	}
	life, cancel := context.WithCancel(context.Background())
	c := &ClusterClient{life: life, cancel: cancel, refresh: make(chan struct{}, 1), seeds: append([]string(nil), seeds...), options: options, clients: make(map[string]*Client)}
	if err = c.Refresh(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func (c *ClusterClient) Refresh(ctx context.Context) error {
	return c.refreshMap(ctx, false)
}
func (c *ClusterClient) refreshMap(ctx context.Context, onlyIfNeeded bool) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	stopClose := context.AfterFunc(c.life, cancel)
	defer stopClose()
	select {
	case c.refresh <- struct{}{}:
		defer func() { <-c.refresh }()
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.RLock()
	if onlyIfNeeded && !c.refreshNeeded {
		c.mu.RUnlock()
		return nil
	}
	seeds := append([]string(nil), c.seeds...)
	closed := c.closed
	current := c.topology
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	// Query seeds concurrently: a stalled or stale seed cannot hide a newer map.
	type observation struct {
		m   ClusterMap
		err error
	}
	results := make(chan observation, len(seeds))
	for _, seed := range seeds {
		go func(seed string) {
			probe, done := context.WithTimeout(ctx, min(500*time.Millisecond, c.options.Timeout/2))
			defer done()
			m, err := readClusterMap(probe, seed, c.options.ManagementSecurity)
			results <- observation{m, err}
		}(seed)
	}
	var best ClusterMap
	last := error(ErrUnavailable)
	for range seeds {
		r := <-results
		if r.err != nil {
			last = r.err
			continue
		}
		m := r.m
		if current.Epoch > 0 && (m.Cluster != current.Cluster || len(m.Partitions) != len(current.Partitions)) {
			return fmt.Errorf("%w: cluster identity changed", ErrProtocol)
		}
		if m.Epoch < current.Epoch {
			continue
		}
		if (m.Epoch == current.Epoch && !reflect.DeepEqual(m, current)) || (m.Epoch == best.Epoch && !reflect.DeepEqual(m, best)) {
			return fmt.Errorf("%w: conflicting map epoch", ErrProtocol)
		}
		if best.Epoch > 0 && (m.Cluster != best.Cluster || len(m.Partitions) != len(best.Partitions)) {
			return ErrProtocol
		}
		if m.Epoch > best.Epoch {
			best = m
		}
	}
	if best.Epoch == 0 {
		return last
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	for _, n := range best.Nodes {
		if c.clients[n.ID] == nil {
			client, err := NewClient(n.Client, c.options)
			if err != nil {
				return err
			}
			c.clients[n.ID] = client
		}
	}
	// Discovered admin addresses allow recovery even when the original seed fails.
	c.seeds = nil
	seen := map[string]bool{}
	for _, n := range best.Nodes {
		if !seen[n.Admin] {
			c.seeds = append(c.seeds, n.Admin)
			seen[n.Admin] = true
		}
	}
	c.topology = best
	c.refreshNeeded = false
	return nil
}

func readClusterMap(ctx context.Context, seed string, security SecurityOptions) (ClusterMap, error) {
	var zero ClusterMap
	conn, err := security.dial(ctx, seed)
	if err != nil {
		return zero, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if _, err = conn.Write([]byte("{\"op\":\"map\"}\n")); err != nil {
		return zero, err
	}
	line, err := bufio.NewReaderSize(conn, 131072).ReadSlice('\n')
	if err != nil {
		return zero, err
	}
	var reply struct {
		OK     bool `json:"ok"`
		Result struct {
			Map ClusterMap `json:"map"`
		} `json:"result"`
	}
	if err = json.Unmarshal(line, &reply); err != nil || !reply.OK {
		return zero, ErrProtocol
	}
	if err = validateMap(reply.Result.Map); err != nil {
		return zero, err
	}
	return reply.Result.Map, nil
}

func validateMap(m ClusterMap) error {
	if m.Cluster == "" || m.Epoch == 0 || len(m.Partitions) == 0 || len(m.Partitions) > 256 || len(m.Nodes) < 2 || len(m.Nodes) > 32 {
		return ErrProtocol
	}
	ids := map[string]bool{}
	for _, n := range m.Nodes {
		if n.ID == "" || ids[n.ID] {
			return ErrProtocol
		}
		ids[n.ID] = true
		for _, a := range []string{n.Client, n.Admin, n.Replication} {
			if _, _, err := net.SplitHostPort(a); err != nil {
				return ErrProtocol
			}
		}
	}
	for _, p := range m.Partitions {
		if p.Primary == p.Replica || !ids[p.Primary] || !ids[p.Replica] {
			return ErrProtocol
		}
	}
	return nil
}
func transientReadError(err error) bool {
	var network net.Error
	return errors.Is(err, ErrBusy) || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrTimedOut) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network)
}

// call routes one request to its partition's owner within one operation
// budget, retrying where that cannot repeat a mutation. Like Client calls, it
// carries the budget as a deadline rather than a derived context.
func (c *ClusterClient) call(ctx context.Context, r request, replica, readOnly bool) ([]byte, error) {
	if ctx == nil || len(r.key) == 0 {
		return nil, ErrInvalidArgument
	}
	deadline := time.Now().Add(c.options.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	delay := 10 * time.Millisecond
	for {
		if c.life.Err() != nil {
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		c.mu.RLock()
		refreshNeeded := c.refreshNeeded
		c.mu.RUnlock()
		if refreshNeeded {
			if err := c.refreshBy(ctx, deadline); err != nil {
				if c.life.Err() != nil {
					return nil, ErrClosed
				}
				if !readOnly || !transientReadError(err) {
					return nil, err
				}
				c.pause(ctx, deadline, &delay)
				continue
			}
		}
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return nil, ErrClosed
		}
		p := c.topology.Partitions[crc32.ChecksumIEEE(databaseKey(c.options.Database, r.key))%uint32(len(c.topology.Partitions))]
		id := p.Primary
		if replica {
			id = p.Replica
		}
		client := c.clients[id]
		c.mu.RUnlock()
		attempt := deadline
		if readOnly {
			if bound := time.Now().Add(min(500*time.Millisecond, c.options.Timeout/2)); bound.Before(attempt) {
				attempt = bound
			}
		}
		value, err := client.executeBy(ctx, attempt, r)
		if err == nil {
			return value, nil
		}
		retry := errors.Is(err, ErrWrongOwner) || (readOnly && transientReadError(err))
		if retry || transientReadError(err) || errors.Is(err, ErrOutcomeUnknown) {
			c.mu.Lock()
			c.refreshNeeded = true
			c.mu.Unlock()
		}
		// An uncertain mutation must never cross the retry boundary, even when wrapped
		// around another status. Preserve its original error for caller reconciliation.
		if errors.Is(err, ErrOutcomeUnknown) || !retry {
			return value, err
		}
		c.pause(ctx, deadline, &delay)
	}
}

// refreshBy refreshes the map if needed, within the operation's deadline.
func (c *ClusterClient) refreshBy(ctx context.Context, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return c.refreshMap(ctx, true)
}

// pause backs off before a retry, doubling up to 100 ms.
func (c *ClusterClient) pause(ctx context.Context, deadline time.Time, delay *time.Duration) {
	timer := time.NewTimer(min(*delay, max(time.Until(deadline), 0)))
	defer timer.Stop()
	*delay = min(2**delay, 100*time.Millisecond)
	select {
	case <-ctx.Done():
	case <-c.life.Done():
	case <-timer.C:
	}
}

func (c *ClusterClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.call(ctx, request{name: "get", op: opGet, key: key}, false, true)
}

// BatchGet returns the values of keys in order, nil for a miss: one batch per
// owning node, sent concurrently (as Aerospike's batch read). Keys a node
// could not answer for its partition (moved, busy, unavailable) are retried
// after a map refresh within one operation budget.
func (c *ClusterClient) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	fail := func(err error) ([][]byte, error) { return nil, &OpError{Op: "batch get", Err: err} }
	if ctx == nil {
		return fail(ErrInvalidArgument)
	}
	deadline := time.Now().Add(c.options.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	values := make([][]byte, len(keys))
	pending := make([]int, len(keys))
	for i := range pending {
		pending[i] = i
	}
	delay := 10 * time.Millisecond
	for {
		if c.life.Err() != nil {
			return fail(ErrClosed)
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if !time.Now().Before(deadline) {
			return fail(context.DeadlineExceeded)
		}
		c.mu.RLock()
		refreshNeeded := c.refreshNeeded
		c.mu.RUnlock()
		if refreshNeeded {
			if err := c.refreshBy(ctx, deadline); err != nil {
				if c.life.Err() != nil {
					return fail(ErrClosed)
				}
				if !transientReadError(err) {
					return fail(err)
				}
				c.pause(ctx, deadline, &delay)
				continue
			}
		}
		// Group the pending keys by their partition's primary.
		groups := map[*Client][]int{}
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return fail(ErrClosed)
		}
		for _, i := range pending {
			partitions := c.topology.Partitions
			p := partitions[crc32.ChecksumIEEE(databaseKey(c.options.Database, keys[i]))%uint32(len(partitions))]
			client := c.clients[p.Primary]
			groups[client] = append(groups[client], i)
		}
		c.mu.RUnlock()
		type outcome struct {
			indexes []int
			entries []batchEntry
			err     error
		}
		outcomes := make(chan outcome, len(groups))
		for client, indexes := range groups {
			go func(client *Client, indexes []int) {
				batch := make([][]byte, len(indexes))
				for j, i := range indexes {
					batch[j] = keys[i]
				}
				entries, err := client.batchEntries(ctx, deadline, batch)
				outcomes <- outcome{indexes, entries, err}
			}(client, indexes)
		}
		var retry []int
		var failure error
		refresh := false
		for range groups {
			o := <-outcomes
			if o.err != nil {
				if transientReadError(o.err) {
					refresh = true
					retry = append(retry, o.indexes...)
				} else if failure == nil {
					failure = o.err
				}
				continue
			}
			for j, i := range o.indexes {
				switch status := o.entries[j].status; status {
				case StatusOK:
					values[i] = o.entries[j].value
				case StatusNotFound:
				// Moved, busy or briefly unavailable: ask again.
				case StatusBusy, StatusUnavailable, StatusTimedOut, StatusWrongOwner:
					refresh = refresh || status != StatusBusy
					retry = append(retry, i)
				default:
					if failure == nil {
						failure = &ServerError{Status: status}
					}
				}
			}
		}
		if failure != nil {
			return fail(failure)
		}
		if len(retry) == 0 {
			return values, nil
		}
		if refresh {
			c.mu.Lock()
			c.refreshNeeded = true
			c.mu.Unlock()
		}
		pending = retry
		c.pause(ctx, deadline, &delay)
	}
}

// GetVersioned is Client.GetVersioned on the key's primary.
func (c *ClusterClient) GetVersioned(ctx context.Context, key []byte) ([]byte, uint64, error) {
	body, err := c.call(ctx, request{name: "get", op: opGet, key: key, options: writeOptions{report: true}}, false, true)
	return splitVersion("get", body, err)
}

// PutIf is Client.PutIf on the key's primary. Like other mutations it is
// retried only on WrongOwner.
func (c *ClusterClient) PutIf(ctx context.Context, key, value []byte, expected uint64, options ...WriteOption) (uint64, error) {
	parsed, err := parseWriteOptions(options)
	if err != nil {
		return 0, &OpError{Op: "put", Err: err}
	}
	parsed.report, parsed.expect, parsed.expected = true, true, expected
	body, err := c.call(ctx, request{name: "put", op: opPut, key: key, value: value, options: parsed}, false, false)
	_, version, err := splitVersion("put", body, err)
	return version, err
}

// DeleteIf is Client.DeleteIf on the key's primary.
func (c *ClusterClient) DeleteIf(ctx context.Context, key []byte, expected uint64, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err == nil && (parsed.ttlSet || expected == 0) {
		err = fmt.Errorf("%w: TTL, or an expected version of 0", ErrInvalidArgument)
	}
	if err != nil {
		return &OpError{Op: "delete", Err: err}
	}
	parsed.report, parsed.expect, parsed.expected = true, true, expected
	_, err = c.call(ctx, request{name: "delete", op: opDelete, key: key, options: parsed}, false, false)
	return err
}

// GetReplica explicitly permits stale reads. Missing keys may reflect replica lag.
func (c *ClusterClient) GetReplica(ctx context.Context, key []byte) ([]byte, error) {
	return c.call(ctx, request{name: "get", op: opGet, key: key}, true, true)
}
func (c *ClusterClient) Put(ctx context.Context, key, value []byte, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err != nil {
		return &OpError{Op: "put", Err: err}
	}
	_, err = c.call(ctx, request{name: "put", op: opPut, key: key, value: value, options: parsed}, false, false)
	return err
}
func (c *ClusterClient) Delete(ctx context.Context, key []byte, options ...WriteOption) error {
	parsed, err := parseWriteOptions(options)
	if err == nil && parsed.ttlSet {
		err = fmt.Errorf("%w: TTL is only valid for Put", ErrInvalidArgument)
	}
	if err != nil {
		return &OpError{Op: "delete", Err: err}
	}
	_, err = c.call(ctx, request{name: "delete", op: opDelete, key: key, options: parsed}, false, false)
	return err
}
func (c *ClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	for _, n := range c.clients {
		_ = n.Close()
	}
	return nil
}

// Map returns a detached copy for diagnostics.
func (c *ClusterClient) Map() ClusterMap {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := c.topology
	m.Nodes = append([]ClusterNode(nil), m.Nodes...)
	m.Partitions = append([]PartitionOwners(nil), m.Partitions...)
	return m
}
