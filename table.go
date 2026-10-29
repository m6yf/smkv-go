package smkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"unicode/utf8"
)

const (
	opGetRow    byte = 4
	opPutRow    byte = 5
	opUpdateRow byte = 6
	opDeleteRow byte = 7
	// opIncrementRow adds to integer or double columns (row-increments.md).
	opIncrementRow byte = 10
	maxColumns          = 64
)

// Columns contains named values: []byte, string, int64 (or int), float64, or bool.
// A missing column differs from a present empty value; nil is not a column value.
type Columns map[string]any

// Table is a lightweight, concurrency-safe handle. It does not create a catalog
// entry or own connections. Close the Client or ClusterClient that created it.
type Table struct {
	client  *Client
	cluster *ClusterClient
	name    string
	limits  Options
	call    func(context.Context, bool, byte, []byte, []byte, writeOptions) ([]byte, error)
}

// Table selects a schemaless table; its name is validated when an operation runs.
func (c *Client) Table(name string) *Table {
	return &Table{client: c, name: name, limits: c.options, call: func(ctx context.Context, replica bool, op byte, key, value []byte, opts writeOptions) ([]byte, error) {
		return c.execute(ctx, request{name: "table", op: op, key: key, value: value, options: opts})
	}}
}

// Table selects a table whose complete (table, key) identity determines routing.
func (c *ClusterClient) Table(name string) *Table {
	return &Table{cluster: c, name: name, limits: c.options, call: func(ctx context.Context, replica bool, op byte, key, value []byte, opts writeOptions) ([]byte, error) {
		return c.call(ctx, request{name: "table", op: op, key: key, value: value, options: opts}, replica, op == opGetRow)
	}}
}

func validName(name string) bool      { return len(name) > 0 && len(name) <= 64 && utf8.ValidString(name) }
func invalidRow(message string) error { return fmt.Errorf("%w: %s", ErrInvalidArgument, message) }
func (t *Table) key(key []byte) ([]byte, error) {
	if !validName(t.name) || len(key) == 0 || 2+len(t.name)+len(key) > t.limits.MaxKeyBytes {
		return nil, invalidRow("invalid table name or encoded key length")
	}
	b := make([]byte, 2, 2+len(t.name)+len(key))
	binary.LittleEndian.PutUint16(b, uint16(len(t.name)))
	b = append(b, t.name...)
	return append(b, key...), nil
}
func appendName(b []byte, name string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(name)))
	return append(b, name...)
}
func encodeNames(names []string) ([]byte, error) {
	if len(names) > maxColumns {
		return nil, invalidRow("too many column names")
	}
	b := binary.LittleEndian.AppendUint16(nil, uint16(len(names)))
	seen := map[string]bool{}
	for _, n := range names {
		if !validName(n) || seen[n] {
			return nil, invalidRow("invalid or duplicate column name")
		}
		seen[n] = true
		b = appendName(b, n)
	}
	return b, nil
}
func columnBytes(v any) (byte, []byte, error) {
	switch v := v.(type) {
	case []byte:
		return 1, v, nil
	case string:
		if !utf8.ValidString(v) {
			return 0, nil, invalidRow("string is not UTF-8")
		}
		return 2, []byte(v), nil
	case int:
		return 3, binary.LittleEndian.AppendUint64(nil, uint64(int64(v))), nil
	case int64:
		return 3, binary.LittleEndian.AppendUint64(nil, uint64(v)), nil
	case float64:
		return 4, binary.LittleEndian.AppendUint64(nil, math.Float64bits(v)), nil
	case bool:
		if v {
			return 5, []byte{1}, nil
		}
		return 5, []byte{0}, nil
	default:
		return 0, nil, invalidRow("unsupported column type")
	}
}
func encodeColumns(columns Columns, limit int) ([]byte, error) {
	if len(columns) > maxColumns {
		return nil, invalidRow("too many columns")
	}
	names := make([]string, 0, len(columns))
	for n := range columns {
		if !validName(n) {
			return nil, invalidRow("invalid column name")
		}
		names = append(names, n)
	}
	sort.Strings(names)
	b := binary.LittleEndian.AppendUint16(nil, uint16(len(names)))
	for _, n := range names {
		switch v := columns[n].(type) {
		case string:
			if len(v) > limit {
				return nil, invalidRow("encoded row exceeds value limit")
			}
		case []byte:
			if len(v) > limit {
				return nil, invalidRow("encoded row exceeds value limit")
			}
		}
		kind, value, err := columnBytes(columns[n])
		if err != nil {
			return nil, err
		}
		if len(value) > limit || len(b)+7+len(n)+len(value) > limit {
			return nil, invalidRow("encoded row exceeds value limit")
		}
		b = appendName(b, n)
		b = append(b, kind)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	if len(b) > limit {
		return nil, invalidRow("encoded row exceeds value limit")
	}
	return b, nil
}

type rowReader struct {
	b   []byte
	err error
}

func (r *rowReader) take(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.err = ErrProtocol
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
func (r *rowReader) u16() int {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint16(b))
}
func decodeColumns(b []byte) (Columns, error) {
	r := rowReader{b: b}
	n := r.u16()
	if n > maxColumns {
		return nil, ErrProtocol
	}
	columns := Columns{}
	for i := 0; i < n; i++ {
		name := string(r.take(r.u16()))
		h := r.take(5)
		if r.err != nil || !validName(name) || h == nil {
			return nil, ErrProtocol
		}
		if _, exists := columns[name]; exists {
			return nil, ErrProtocol
		}
		length := uint64(binary.LittleEndian.Uint32(h[1:]))
		if length > uint64(len(r.b)) {
			return nil, ErrProtocol
		}
		v := r.take(int(length))
		switch h[0] {
		case 1:
			columns[name] = append([]byte{}, v...)
		case 2:
			if !utf8.Valid(v) {
				return nil, ErrProtocol
			}
			columns[name] = string(v)
		case 3:
			if len(v) != 8 {
				return nil, ErrProtocol
			}
			columns[name] = int64(binary.LittleEndian.Uint64(v))
		case 4:
			if len(v) != 8 {
				return nil, ErrProtocol
			}
			columns[name] = math.Float64frombits(binary.LittleEndian.Uint64(v))
		case 5:
			if len(v) != 1 || v[0] > 1 {
				return nil, ErrProtocol
			}
			columns[name] = v[0] == 1
		default:
			return nil, ErrProtocol
		}
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, ErrProtocol
	}
	return columns, nil
}

// Get returns all columns when projection is empty. Missing projected columns
// are omitted; a missing or expired row returns ErrNotFound.
func (t *Table) Get(ctx context.Context, key []byte, projection ...string) (Columns, error) {
	return t.get(ctx, false, key, projection)
}

// GetReplica allows stale reads. Cluster handles route to the replica; standalone
// handles read their configured server, just like Get.
func (t *Table) GetReplica(ctx context.Context, key []byte, projection ...string) (Columns, error) {
	return t.get(ctx, true, key, projection)
}
func (t *Table) get(ctx context.Context, replica bool, key []byte, projection []string) (Columns, error) {
	k, err := t.key(key)
	if err != nil {
		return nil, err
	}
	body, err := encodeNames(projection)
	if err != nil {
		return nil, err
	}
	b, err := t.call(ctx, replica, opGetRow, k, body, writeOptions{})
	if err != nil {
		return nil, err
	}
	return decodeColumns(b)
}

// Put replaces the entire row. Omitted TTL means no expiration.
func (t *Table) Put(ctx context.Context, key []byte, columns Columns, options ...WriteOption) error {
	body, err := encodeColumns(columns, t.limits.MaxValueBytes)
	if err != nil {
		return err
	}
	return t.mutate(ctx, opPutRow, key, body, options)
}

// Update atomically sets and removes selected columns of an existing row. It
// preserves other columns and expiration unless WithTTL is supplied. Set/remove
// overlap is invalid. No automatic retry occurs after an uncertain outcome.
func (t *Table) Update(ctx context.Context, key []byte, set Columns, remove []string, options ...WriteOption) error {
	for _, n := range remove {
		if _, ok := set[n]; ok {
			return invalidRow("column cannot be set and removed together")
		}
	}
	body, err := encodeColumns(set, t.limits.MaxValueBytes)
	if err != nil {
		return err
	}
	deleted, err := encodeNames(remove)
	if err != nil {
		return err
	}
	if len(body)+len(deleted) > t.limits.MaxValueBytes {
		return invalidRow("update exceeds value limit")
	}
	return t.mutate(ctx, opUpdateRow, key, append(body, deleted...), options)
}

// Increment atomically adds each amount to its column: int64 (or int)
// amounts to integer columns, float64 to double ones. A missing row or column
// is created holding the amount; other columns stay. It returns the
// incremented columns' new values. Without WithTTL the row keeps its
// expiration (a new row never expires). Overflow, or a column holding another
// type, fails with StatusInvalidRequest and writes nothing. Increment is not
// idempotent: an OpError with OutcomeUnknown may have been applied, and it is
// never retried automatically (docs/proposals/row-increments.md).
func (t *Table) Increment(ctx context.Context, key []byte, amounts Columns, options ...WriteOption) (Columns, error) {
	if len(amounts) == 0 {
		return nil, invalidRow("Increment needs at least one amount")
	}
	for name, amount := range amounts {
		switch amount.(type) {
		case int, int64, float64:
		default:
			return nil, invalidRow(fmt.Sprintf("amount for %q is not an integer or a float64", name))
		}
	}
	body, err := encodeColumns(amounts, t.limits.MaxValueBytes)
	if err != nil {
		return nil, err
	}
	k, err := t.key(key)
	if err != nil {
		return nil, err
	}
	opts, err := parseWriteOptions(options)
	if err != nil {
		return nil, err
	}
	reply, err := t.call(ctx, false, opIncrementRow, k, body, opts)
	if err != nil {
		return nil, err
	}
	return decodeColumns(reply)
}

// Delete idempotently deletes the row. Sync and replica acknowledgment options apply; TTL is rejected.
func (t *Table) Delete(ctx context.Context, key []byte, options ...WriteOption) error {
	return t.mutate(ctx, opDeleteRow, key, nil, options)
}
func (t *Table) mutate(ctx context.Context, op byte, key, body []byte, options []WriteOption) error {
	k, err := t.key(key)
	if err != nil {
		return err
	}
	opts, err := parseWriteOptions(options)
	if err != nil {
		return err
	}
	if op == opDeleteRow && opts.ttlSet {
		return invalidRow("Delete does not accept TTL")
	}
	_, err = t.call(ctx, false, op, k, body, opts)
	return err
}
