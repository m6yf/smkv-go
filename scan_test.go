package smkv

import (
	"encoding/json"
	"math"
	"testing"
)

func TestScanTypedValuesAndInvalidFilters(t *testing.T) {
	for _, v := range []any{int64(math.MaxInt64), int64(math.MinInt64), "abc", true, 1.25} {
		wire, e := toScanValue(v)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(wire)
		var read scanValue
		if e = json.Unmarshal(b, &read); e != nil {
			t.Fatal(e)
		}
		got, e := read.decode()
		if e != nil || got != v {
			t.Fatalf("round trip %v => %v: %v", v, got, e)
		}
	}
	for _, o := range []ScanOptions{{Limit: 101}, {DeadlineMS: -1}, {Filters: []ScanFilter{{Column: "x", Op: "exists", Value: true}}}, {Filters: []ScanFilter{{Column: "x", Op: "ge", Value: "1"}}}, {Projection: []string{"x", "x"}}} {
		if _, e := scanParameters(o); e == nil {
			t.Fatalf("invalid accepted: %+v", o)
		}
	}
}
