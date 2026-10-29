package smkv

import (
	"context"
	"errors"
	"testing"
)

func TestReplicaAcknowledgmentFlags(t *testing.T) {
	for _, tc := range []struct {
		options []WriteOption
		flags   byte
	}{
		{nil, 0}, {[]WriteOption{WithSync()}, 1},
		{[]WriteOption{WithReplicaAck()}, 2},
		{[]WriteOption{WithReplicaAck(), WithSync()}, 3},
		{[]WriteOption{WithDurableReplicaAck()}, 6},
		{[]WriteOption{WithDurableReplicaAck(), WithReplicaAck(), WithSync()}, 7},
	} {
		options, err := parseWriteOptions(tc.options)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range []byte{opPut, opDelete, opPutRow, opUpdateRow, opDeleteRow} {
			if got := requestHeader(op, []byte("key"), nil, options)[5]; got != tc.flags {
				t.Fatalf("op %d: flags %d, want %d", op, got, tc.flags)
			}
		}
	}
}

func TestConfirmedMutationsDoNotRetryUnknownOutcomes(t *testing.T) {
	for _, ack := range []WriteOption{WithReplicaAck(), WithDurableReplicaAck()} {
		for _, status := range []int{int(StatusUnavailable), int(StatusTimedOut), -1} {
			c, oldCalls, newCalls := retryFixture(t, status, true)
			err := c.Put(context.Background(), []byte("key"), []byte("value"), ack)
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("status %d: %v", status, err)
			}
			if oldCalls.Load() != 1 || newCalls.Load() != 0 {
				t.Fatal("confirmed mutation retried after unknown outcome")
			}
			c.Close()
		}
	}
}
