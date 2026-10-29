package smkv

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

const opScan byte = 8

// ScanFilter is one AND condition. Op is eq, lt, le, gt, ge, or exists.
// Value uses the same types as Columns; exists requires nil.
type ScanFilter struct {
	Column string
	Op     string
	Value  any
}

// ScanOptions bounds one page. Empty pages may have a nonempty Cursor.
// A cursor is bound to the table, filters and projection; do not edit it.
type ScanOptions struct {
	Projection []string
	Filters    []ScanFilter
	Limit      int
	DeadlineMS int
	Cursor     string
}
type ScanRow struct {
	Key     []byte
	Columns Columns
}
type ScanPage struct {
	Rows      []ScanRow
	Cursor    string
	Examined  int
	Partition uint16
}
type scanValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}
type scanFilter struct {
	Column string     `json:"column"`
	Op     string     `json:"op"`
	Value  *scanValue `json:"value,omitempty"`
}
type scanQuery struct {
	Database   string       `json:"database,omitempty"`
	Partition  uint16       `json:"partition"`
	Cluster    string       `json:"cluster"`
	Epoch      uint64       `json:"epoch"`
	Projection []string     `json:"projection"`
	Filters    []scanFilter `json:"filters"`
	Limit      int          `json:"limit"`
	DeadlineMS int          `json:"deadline_ms"`
	Cursor     string       `json:"cursor,omitempty"`
}
type scanCursor struct {
	Cluster   string `json:"cluster"`
	Epoch     uint64 `json:"epoch"`
	Partition uint16 `json:"partition"`
	Local     string `json:"local"`
	Signature string `json:"signature"`
}

func toScanValue(v any) (*scanValue, error) {
	if _, _, err := columnBytes(v); err != nil {
		return nil, err
	}
	var kind string
	var value any
	switch v := v.(type) {
	case []byte:
		kind = "bytes"
		value = hex.EncodeToString(v)
	case string:
		kind = "string"
		value = v
	case int:
		kind = "int"
		value = strconv.FormatInt(int64(v), 10)
	case int64:
		kind = "int"
		value = strconv.FormatInt(v, 10)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, invalidRow("non-finite filter")
		}
		kind = "float"
		value = v
	case bool:
		kind = "bool"
		value = v
	default:
		return nil, invalidRow("invalid scan value")
	}
	b, e := json.Marshal(value)
	return &scanValue{kind, b}, e
}
func (v scanValue) decode() (any, error) {
	var s string
	switch v.Type {
	case "bytes", "string", "int":
		if e := json.Unmarshal(v.Value, &s); e != nil {
			return nil, e
		}
		if v.Type == "bytes" {
			return hex.DecodeString(s)
		}
		if v.Type == "int" {
			return strconv.ParseInt(s, 10, 64)
		}
		return s, nil
	case "float":
		var n float64
		e := json.Unmarshal(v.Value, &n)
		return n, e
	case "bool":
		var b bool
		e := json.Unmarshal(v.Value, &b)
		return b, e
	default:
		return nil, fmt.Errorf("%w: unknown scan value", ErrProtocol)
	}
}
func scanParameters(o ScanOptions) (scanQuery, error) {
	if o.Limit == 0 {
		o.Limit = 50
	}
	if o.DeadlineMS == 0 {
		o.DeadlineMS = 1000
	}
	q := scanQuery{Projection: append([]string{}, o.Projection...), Filters: []scanFilter{}, Limit: o.Limit, DeadlineMS: o.DeadlineMS}
	if o.Limit < 1 || o.Limit > 100 || o.DeadlineMS < 1 || o.DeadlineMS > 5000 || len(o.Filters) > 16 || len(o.Cursor) > 20000 {
		return q, invalidRow("invalid scan bounds")
	}
	if _, e := encodeNames(o.Projection); e != nil {
		return q, e
	}
	for _, f := range o.Filters {
		if !validName(f.Column) {
			return q, invalidRow("invalid filter column")
		}
		if f.Op != "eq" && f.Op != "lt" && f.Op != "le" && f.Op != "gt" && f.Op != "ge" && f.Op != "exists" {
			return q, invalidRow("invalid filter operator")
		}
		var value *scanValue
		if f.Op == "exists" {
			if f.Value != nil {
				return q, invalidRow("exists does not accept a value")
			}
		} else {
			var e error
			value, e = toScanValue(f.Value)
			if e != nil {
				return q, e
			}
			if f.Op != "eq" && value.Type != "int" && value.Type != "float" {
				return q, invalidRow("range requires numeric value")
			}
		}
		q.Filters = append(q.Filters, scanFilter{f.Column, f.Op, value})
	}
	return q, nil
}
func scanSignature(table string, q scanQuery) string {
	b, _ := json.Marshal([]any{q.Database, table, q.Projection, q.Filters})
	return string(b)
}
func (t *Table) scanOn(ctx context.Context, c *Client, q scanQuery) (ScanPage, error) {
	var page ScanPage
	b, e := json.Marshal(q)
	if e != nil {
		return page, e
	}
	if len(b) > 8192 {
		return page, invalidRow("scan query too large")
	}
	raw, e := c.execute(ctx, request{name: "scan", op: opScan, key: []byte(t.name), value: b})
	if e != nil {
		return page, e
	}
	var wire struct {
		Rows []struct {
			Key     string               `json:"key"`
			Columns map[string]scanValue `json:"columns"`
		} `json:"rows"`
		Cursor    string `json:"cursor"`
		Examined  int    `json:"examined"`
		Partition uint16 `json:"partition"`
	}
	if e = json.Unmarshal(raw, &wire); e != nil {
		return page, fmt.Errorf("%w: invalid scan page", ErrProtocol)
	}
	if wire.Partition != q.Partition || len(wire.Rows) > q.Limit || wire.Examined > 128 || len(wire.Cursor) > 8192 {
		return page, fmt.Errorf("%w: invalid scan bounds", ErrProtocol)
	}
	page.Cursor = wire.Cursor
	page.Examined = wire.Examined
	page.Partition = wire.Partition
	for _, r := range wire.Rows {
		key, e := hex.DecodeString(r.Key)
		if e != nil {
			return ScanPage{}, fmt.Errorf("%w: invalid scan key", ErrProtocol)
		}
		cols := Columns{}
		for name, value := range r.Columns {
			v, e := value.decode()
			if e != nil {
				return ScanPage{}, fmt.Errorf("%w: invalid scan column", ErrProtocol)
			}
			cols[name] = v
		}
		page.Rows = append(page.Rows, ScanRow{key, cols})
	}
	return page, nil
}

// ScanPartition scans one explicitly selected standalone partition. Cursor expires
// on restart or compaction; concurrent writes can cause omissions or duplicates.
func (t *Table) ScanPartition(ctx context.Context, partition uint16, o ScanOptions) (ScanPage, error) {
	if t.client == nil || !validName(t.name) {
		return ScanPage{}, invalidRow("ScanPartition requires a standalone client and valid table")
	}
	q, e := scanParameters(o)
	if e != nil {
		return ScanPage{}, e
	}
	q.Database = t.client.options.Database
	q.Partition = partition
	q.Cursor = o.Cursor
	return t.scanOn(ctx, t.client, q)
}

// Scan returns one page across cluster primaries, advancing only after a partition
// completes. It never retries a page automatically. Retry Busy with the same cursor
// after 100ms; topology or storage changes require restarting the scan.
func (t *Table) Scan(ctx context.Context, o ScanOptions) (ScanPage, error) {
	if ctx == nil || t.cluster == nil || !validName(t.name) {
		return ScanPage{}, invalidRow("Scan requires a cluster client and valid table")
	}
	q, e := scanParameters(o)
	if e != nil {
		return ScanPage{}, e
	}
	c := t.cluster
	q.Database = c.options.Database
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	if e = c.Refresh(ctx); e != nil {
		return ScanPage{}, e
	}
	m := c.Map()
	cursor := scanCursor{Cluster: m.Cluster, Epoch: m.Epoch, Signature: scanSignature(t.name, q)}
	if o.Cursor != "" {
		b, e := hex.DecodeString(o.Cursor)
		if e != nil || json.Unmarshal(b, &cursor) != nil {
			return ScanPage{}, invalidRow("invalid scan cursor")
		}
		if cursor.Cluster != m.Cluster || cursor.Epoch != m.Epoch || cursor.Signature != scanSignature(t.name, q) {
			return ScanPage{}, invalidRow("scan changed; restart scan")
		}
	}
	if int(cursor.Partition) >= len(m.Partitions) {
		return ScanPage{}, invalidRow("invalid scan partition")
	}
	q.Partition = cursor.Partition
	q.Cluster = m.Cluster
	q.Epoch = m.Epoch
	q.Cursor = cursor.Local
	c.mu.RLock()
	client := c.clients[m.Partitions[q.Partition].Primary]
	c.mu.RUnlock()
	if client == nil {
		return ScanPage{}, ErrClosed
	}
	page, e := t.scanOn(ctx, client, q)
	if e != nil {
		return ScanPage{}, e
	}
	cursor.Local = page.Cursor
	if page.Cursor == "" {
		cursor.Partition++
	}
	page.Cursor = ""
	if int(cursor.Partition) < len(m.Partitions) {
		b, _ := json.Marshal(cursor)
		page.Cursor = hex.EncodeToString(b)
	}
	return page, nil
}
