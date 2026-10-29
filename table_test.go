package smkv

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestColumnEncodingGoldenAndMalformed(t *testing.T) {
	golden, _ := hex.DecodeString("02000100610308000000feffffffffffffff010062050100000001")
	want := Columns{"a": int64(-2), "b": true}
	b, err := encodeColumns(want, 1024)
	if err != nil || !bytes.Equal(b, golden) {
		t.Fatalf("encoding %x %v", b, err)
	}
	got, err := decodeColumns(golden)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("decode %v %v", got, err)
	}
	for n := 0; n < len(golden); n++ {
		if _, err := decodeColumns(golden[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	bad := append([]byte{}, golden...)
	bad[5] = 99
	if _, err := decodeColumns(bad); err == nil {
		t.Fatal("accepted unknown type")
	}
	bad = append(append([]byte{}, golden...), 0)
	if _, err := decodeColumns(bad); err == nil {
		t.Fatal("accepted trailing byte")
	}
	for _, v := range []any{nil, uint64(1), []string{"x"}, "\xff"} {
		if _, err := encodeColumns(Columns{"x": v}, 1024); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("accepted %T", v)
		}
	}
	if _, err := encodeColumns(Columns{"x": bytes.Repeat([]byte{1}, 100)}, 10); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("accepted oversized row")
	}
}

func TestTableValidationAndOperationSemantics(t *testing.T) {
	opts, _ := (Options{}).defaults()
	calls := 0
	table := &Table{name: "users", limits: opts, call: func(ctx context.Context, replica bool, op byte, key, value []byte, w writeOptions) ([]byte, error) {
		calls++
		if !bytes.Equal(key, []byte{5, 0, 'u', 's', 'e', 'r', 's', 'k'}) {
			t.Fatalf("key %x", key)
		}
		if op != opUpdateRow || w.ttlSet || replica {
			t.Fatalf("invalid update options %+v", w)
		}
		return nil, nil
	}}
	if err := table.Update(context.Background(), []byte("k"), Columns{"x": int64(1)}, nil); err != nil {
		t.Fatal(err)
	}
	if err := table.Update(context.Background(), []byte("k"), Columns{"x": true}, []string{"x"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("conflict accepted")
	}
	if err := table.Delete(context.Background(), []byte("k"), WithTTL(0)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("delete TTL accepted")
	}
	if _, err := table.Get(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("empty key accepted")
	}
	if _, err := table.Get(context.Background(), []byte("k"), "x", "x"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("duplicate projection accepted")
	}
	if calls != 1 {
		t.Fatalf("validation performed network calls: %d", calls)
	}
}

func TestRowResponseStatusRules(t *testing.T) {
	notFound := []byte{'S', 'K', 'R', '1', 1, 0, 0, 0, 0, 0, 0, 0}
	for _, op := range []byte{opGetRow, opUpdateRow} {
		if _, err := readResponse(bytes.NewReader(notFound), new([responseHeaderSize]byte), op, 1024, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, op := range []byte{opPutRow, opDeleteRow} {
		if _, err := readResponse(bytes.NewReader(notFound), new([responseHeaderSize]byte), op, 1024, false); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid NotFound accepted")
		}
	}
}

// Increment sends operation 10 with the amounts as a row, returns the reply's
// columns, and refuses amounts that are not integers or floats before any
// request (docs/proposals/row-increments.md).
func TestIncrementFramesAmountsAndDecodesTheNewValues(t *testing.T) {
	opts, _ := (Options{}).defaults()
	reply, _ := encodeColumns(Columns{"hits": int64(12)}, 1024)
	calls := 0
	table := &Table{name: "c", limits: opts, call: func(ctx context.Context, replica bool, op byte, key, value []byte, w writeOptions) ([]byte, error) {
		calls++
		if op != opIncrementRow || replica || !w.ttlSet || w.ttlMS != 5000 {
			t.Fatalf("op %d replica %v options %+v", op, replica, w)
		}
		amounts, err := decodeColumns(value)
		if err != nil || !reflect.DeepEqual(amounts, Columns{"hits": int64(2), "load": 0.5}) {
			t.Fatalf("amounts %v %v", amounts, err)
		}
		return reply, nil
	}}
	got, err := table.Increment(context.Background(), []byte("k"), Columns{"hits": 2, "load": 0.5}, WithTTL(5*time.Second))
	if err != nil || !reflect.DeepEqual(got, Columns{"hits": int64(12)}) {
		t.Fatalf("increment %v %v", got, err)
	}
	for _, amounts := range []Columns{{}, {"x": "1"}, {"x": true}, {"x": []byte{1}}} {
		if _, err := table.Increment(context.Background(), []byte("k"), amounts); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("accepted %v: %v", amounts, err)
		}
	}
	if calls != 1 {
		t.Fatalf("%d requests sent", calls)
	}
	// Its OK reply has a body; a plain write's does not.
	ok := []byte{'S', 'K', 'R', '1', 0, 0, 0, 0, byte(len(reply)), 0, 0, 0}
	if r, err := readResponse(bytes.NewReader(append(ok, reply...)), new([responseHeaderSize]byte), opIncrementRow, 1024, false); err != nil || !bytes.Equal(r.body, reply) {
		t.Fatalf("increment reply %v %v", r, err)
	}
	if _, err := readResponse(bytes.NewReader(append(ok, reply...)), new([responseHeaderSize]byte), opUpdateRow, 1024, false); !errors.Is(err, ErrProtocol) {
		t.Fatal("update reply body accepted")
	}
}
