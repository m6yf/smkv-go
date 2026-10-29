package smkv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"strconv"
	"testing"
)

func TestSharedSDKProtocolVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Requests []struct {
			Op    byte   `json:"op"`
			Flags byte   `json:"flags"`
			Key   string `json:"key_hex"`
			Value string `json:"value_hex"`
			TTL   string `json:"ttl_ms"`
			Frame string `json:"frame_hex"`
		} `json:"requests"`
		Row string `json:"row_hex"`
		Key string `json:"row_key_hex"`
		CRC uint32 `json:"row_crc32"`
	}
	if err = json.Unmarshal(b, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors.Requests {
		key, _ := hex.DecodeString(v.Key)
		value, _ := hex.DecodeString(v.Value)
		ttl, err := strconv.ParseUint(v.TTL, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		h := requestHeader(v.Op, key, value, writeOptions{ttlSet: ttl != ^uint64(0), ttlMS: ttl, sync: v.Flags&1 != 0, replicaAck: v.Flags&2 != 0, durableReplicaAck: v.Flags&4 != 0})
		frame := append(append(h[:], key...), value...)
		if hex.EncodeToString(frame) != v.Frame {
			t.Fatal("wire frame mismatch")
		}
	}
	row, _ := hex.DecodeString(vectors.Row)
	columns, err := decodeColumns(row)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeColumns(columns, 1024)
	if err != nil || hex.EncodeToString(encoded) != vectors.Row {
		t.Fatal("row encoding mismatch", err)
	}
	key, _ := hex.DecodeString(vectors.Key)
	if crc32.ChecksumIEEE(key) != vectors.CRC {
		t.Fatal("routing mismatch")
	}
}

func TestDatabaseWireIdentity(t *testing.T) {
	b, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Database struct {
			Name  string `json:"name"`
			Key   string `json:"key_hex"`
			Wire  string `json:"wire_key_hex"`
			CRC   uint32 `json:"crc32"`
			Frame string `json:"get_frame_hex"`
		} `json:"database"`
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	raw, _ := hex.DecodeString(v.Database.Key)
	key := databaseKey(v.Database.Name, raw)
	if hex.EncodeToString(key) != v.Database.Wire || crc32.ChecksumIEEE(key) != v.Database.CRC {
		t.Fatal("database routing identity mismatch")
	}
	header := requestHeader(1, key, nil, writeOptions{database: true})
	frame := append(header[:], key...)
	if hex.EncodeToString(frame) != v.Database.Frame {
		t.Fatal("database frame mismatch")
	}
	for _, name := range []string{"a/b", "a\n", "é", string(make([]byte, 65))} {
		if _, err := (Options{Database: name}).defaults(); err == nil {
			t.Fatal("accepted invalid database")
		}
	}
}

func TestBatchVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Batch struct {
			Requests []struct {
				Database string   `json:"database"`
				Keys     []string `json:"keys_hex"`
				Frame    string   `json:"frame_hex"`
			} `json:"requests"`
			Response struct {
				Body    string `json:"body_hex"`
				Entries []struct {
					Status Status `json:"status"`
					Value  string `json:"value_hex"`
				} `json:"entries"`
			} `json:"response"`
		} `json:"batch"`
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for _, r := range v.Batch.Requests {
		keys := make([][]byte, len(r.Keys))
		chunk := make([]int, len(r.Keys))
		for i, k := range r.Keys {
			keys[i], _ = hex.DecodeString(k)
			chunk[i] = i
		}
		list := batchList(r.Database, keys, chunk)
		header := requestHeader(opBatchGet, list, nil, writeOptions{database: r.Database != ""})
		if hex.EncodeToString(append(header[:], list...)) != r.Frame {
			t.Fatalf("batch frame mismatch for %q", r.Database)
		}
	}
	body, _ := hex.DecodeString(v.Batch.Response.Body)
	entries, err := decodeBatch(body, len(v.Batch.Response.Entries))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range v.Batch.Response.Entries {
		value, _ := hex.DecodeString(want.Value)
		if entries[i].status != want.Status || !bytes.Equal(entries[i].value, value) {
			t.Fatalf("entry %d: %+v", i, entries[i])
		}
	}
}

func TestVersionVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Versions struct {
			Requests []struct {
				Op       byte    `json:"op"`
				Key      string  `json:"key_hex"`
				Expected *string `json:"expected"`
				Value    string  `json:"value_hex"`
				Frame    string  `json:"frame_hex"`
			} `json:"requests"`
			Responses []struct {
				Status  Status  `json:"status"`
				Body    string  `json:"body_hex"`
				Version *string `json:"version"`
				Value   string  `json:"value_hex"`
			} `json:"responses"`
		} `json:"versions"`
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for _, r := range v.Versions.Requests {
		key, _ := hex.DecodeString(r.Key)
		value, _ := hex.DecodeString(r.Value)
		options := writeOptions{report: true}
		if r.Expected != nil {
			expected, err := strconv.ParseUint(*r.Expected, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			options.expect, options.expected = true, expected
			value = append(binary.LittleEndian.AppendUint64(nil, expected), value...)
		}
		header := requestHeader(r.Op, key, value, options)
		if hex.EncodeToString(append(append(header[:], key...), value...)) != r.Frame {
			t.Fatalf("versioned frame mismatch for op %d", r.Op)
		}
	}
	for _, r := range v.Versions.Responses {
		body, _ := hex.DecodeString(r.Body)
		if r.Status == StatusConflict {
			if !errors.Is(&ServerError{Status: r.Status}, ErrConflict) {
				t.Fatal("conflict status does not map to ErrConflict")
			}
			continue
		}
		value, version, err := splitVersion("get", body, nil)
		want, _ := hex.DecodeString(r.Value)
		if err != nil || strconv.FormatUint(version, 10) != *r.Version || !bytes.Equal(value, want) {
			t.Fatalf("versioned reply %q %d %v", value, version, err)
		}
	}
}
