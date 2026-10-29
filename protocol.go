package smkv

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	requestHeaderSize       = 24
	responseHeaderSize      = 12
	maxErrorBytes           = 1024
	opGet              byte = 1
	opPut              byte = 2
	opDelete           byte = 3
	opBatchGet         byte = 9
)

func requestHeader(op byte, key, value []byte, options writeOptions) [requestHeaderSize]byte {
	var header [requestHeaderSize]byte
	copy(header[:4], "SKV1")
	header[4] = op
	if options.sync {
		header[5] = 1
	}
	if options.database {
		header[5] |= 8
	}
	if options.replicaAck {
		header[5] |= 2
	}
	if options.durableReplicaAck {
		header[5] |= 4
	}
	if options.report {
		header[5] |= 16
	}
	if options.expect {
		header[5] |= 32
	}
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(value)))
	ttl := uint64(math.MaxUint64)
	if options.ttlSet {
		ttl = options.ttlMS
	}
	binary.LittleEndian.PutUint64(header[16:24], ttl)
	return header
}

type response struct {
	status Status
	body   []byte
}

// readResponse reads one response, using `header` as scratch space.
// readResponse reads one reply. A successful reply may carry a body of up to
// maxValueBytes for reads, row increments (their new values), and versioned
// requests (whose replies lead with the version).
func readResponse(reader io.Reader, header *[responseHeaderSize]byte, op byte, maxValueBytes int, versioned bool) (response, error) {
	var result response
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return result, err
	}
	if string(header[:4]) != "SKR1" || header[5] != 0 || header[6] != 0 || header[7] != 0 {
		return result, fmt.Errorf("%w: bad magic or reserved bytes", ErrProtocol)
	}
	result.status = Status(header[4])
	length := uint64(binary.LittleEndian.Uint32(header[8:12]))
	var limit uint64
	switch result.status {
	case StatusOK:
		if op == opGet || op == opGetRow || op == opScan || op == opBatchGet || op == opIncrementRow || versioned {
			limit = uint64(maxValueBytes)
			if op == opScan {
				limit = min(limit, 256*1024)
			}
		}
	case StatusNotFound:
		if op != opGet && op != opGetRow && op != opUpdateRow {
			return result, fmt.Errorf("%w: NotFound for mutation", ErrProtocol)
		}
	case StatusBusy, StatusInvalidRequest, StatusStorageError, StatusUnavailable, StatusTimedOut, StatusWrongOwner, StatusCapacity, StatusUnauthorized, StatusConflict:
		limit = maxErrorBytes
	default:
		return result, fmt.Errorf("%w: unknown status %d", ErrProtocol, result.status)
	}
	if length > limit {
		return result, fmt.Errorf("%w: response body exceeds limit", ErrProtocol)
	}
	result.body = make([]byte, int(length))
	if _, err := io.ReadFull(reader, result.body); err != nil {
		return result, err
	}
	return result, nil
}

func databaseKey(database string, key []byte) []byte {
	if database == "" || database == "default" {
		return key
	}
	encoded := make([]byte, 2+len(database)+len(key))
	binary.LittleEndian.PutUint16(encoded, uint16(len(database)))
	copy(encoded[2:], database)
	copy(encoded[2+len(database):], key)
	return encoded
}

// batchEntry is one key's answer in a batch get reply.
type batchEntry struct {
	status Status
	value  []byte
}

// batchList encodes a batch get's key list: the database name when it is not
// the default, the count, then each key with its length.
func batchList(database string, keys [][]byte, chunk []int) []byte {
	size := 4
	if database != "" && database != "default" {
		size += 2 + len(database)
	}
	for _, i := range chunk {
		size += 4 + len(keys[i])
	}
	list := make([]byte, 0, size)
	if database != "" && database != "default" {
		list = binary.LittleEndian.AppendUint16(list, uint16(len(database)))
		list = append(list, database...)
	}
	list = binary.LittleEndian.AppendUint32(list, uint32(len(chunk)))
	for _, i := range chunk {
		list = binary.LittleEndian.AppendUint32(list, uint32(len(keys[i])))
		list = append(list, keys[i]...)
	}
	return list
}

// decodeBatch parses a batch get reply of `count` entries. Values are copied,
// so each belongs to the caller on its own; a hit is never nil.
func decodeBatch(body []byte, count int) ([]batchEntry, error) {
	bad := fmt.Errorf("%w: malformed batch reply", ErrProtocol)
	if len(body) < 4 || binary.LittleEndian.Uint32(body) != uint32(count) {
		return nil, bad
	}
	entries := make([]batchEntry, count)
	at := 4
	for i := range entries {
		if len(body)-at < 5 {
			return nil, bad
		}
		status := Status(body[at])
		length := int(binary.LittleEndian.Uint32(body[at+1:]))
		at += 5
		if status > StatusUnauthorized || (status != StatusOK && length != 0) || length > len(body)-at {
			return nil, bad
		}
		entries[i] = batchEntry{status, append(make([]byte, 0, length), body[at:at+length]...)}
		at += length
	}
	if at != len(body) {
		return nil, bad
	}
	return entries, nil
}
