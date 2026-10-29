package smkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testRequest struct {
	header     [requestHeaderSize]byte
	key, value []byte
}

func readTestRequest(conn net.Conn) (testRequest, error) {
	var request testRequest
	if _, err := io.ReadFull(conn, request.header[:]); err != nil {
		return request, err
	}
	keyLen := binary.LittleEndian.Uint32(request.header[8:12])
	valueLen := binary.LittleEndian.Uint32(request.header[12:16])
	if string(request.header[:4]) != "SKV1" || keyLen > 64*1024 || valueLen > 64*1024*1024 {
		return request, fmt.Errorf("invalid request header")
	}
	request.key, request.value = make([]byte, keyLen), make([]byte, valueLen)
	if _, err := io.ReadFull(conn, request.key); err != nil {
		return request, err
	}
	_, err := io.ReadFull(conn, request.value)
	return request, err
}

func testResponse(status Status, body []byte) []byte {
	frame := make([]byte, responseHeaderSize+len(body))
	copy(frame[:4], "SKR1")
	frame[4] = byte(status)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(body)))
	copy(frame[12:], body)
	return frame
}

// net.Pipe exercises framing and cancellation without binding local sockets.
// Real TCP interoperability lives in the integration-tagged tests.
func pipeClient(t *testing.T, options Options, serve func(net.Conn)) (*Client, *atomic.Int32) {
	t.Helper()
	client, err := NewClient("127.0.0.1:7379", options)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var dials atomic.Int32
	client.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		local, remote := net.Pipe()
		dials.Add(1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer remote.Close()
			_ = remote.SetDeadline(time.Now().Add(5 * time.Second))
			serve(remote)
		}()
		return local, nil
	}
	t.Cleanup(func() { _ = client.Close(); workers.Wait() })
	return client, &dials
}

func waitError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func awaitSignal(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture did not receive request")
	}
}

func TestBinaryRequestsTTLAndSyncMatchWireFormat(t *testing.T) {
	requests := make(chan testRequest, 4)
	client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		for {
			request, err := readTestRequest(conn)
			if err != nil {
				return
			}
			requests <- request
			if _, err := conn.Write(testResponse(StatusOK, nil)); err != nil {
				return
			}
		}
	})
	ctx := context.Background()
	key, value := []byte{0, 255}, []byte{1, 2, 255}
	if err := client.Put(ctx, key, value, WithTTL(1500*time.Microsecond), WithSync()); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	expected := [24]byte{'S', 'K', 'V', '1', 2, 1, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}
	if request.header != expected || !bytes.Equal(request.key, key) || !bytes.Equal(request.value, value) {
		t.Fatalf("wrong request: %+v", request)
	}
	if err := client.Put(ctx, key, nil); err != nil {
		t.Fatal(err)
	}
	request = <-requests
	if ttl := binary.LittleEndian.Uint64(request.header[16:]); ttl != math.MaxUint64 {
		t.Fatalf("omitted TTL = %d", ttl)
	}
	if err := client.Put(ctx, key, value, WithTTL(0)); err != nil {
		t.Fatal(err)
	}
	request = <-requests
	if ttl := binary.LittleEndian.Uint64(request.header[16:]); ttl != 0 {
		t.Fatalf("zero TTL = %d", ttl)
	}
	if err := client.Delete(ctx, key, WithSync()); err != nil {
		t.Fatal(err)
	}
	request = <-requests
	if request.header[4] != 3 || request.header[5] != 1 || len(request.value) != 0 {
		t.Fatal("invalid delete frame")
	}
	if dials.Load() != 1 {
		t.Fatalf("connection was not reused: %d dials", dials.Load())
	}
}

func TestFragmentedAndEmptyResponsesAreDistinctFromMisses(t *testing.T) {
	var responses atomic.Int32
	client, _ := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		for {
			if _, err := readTestRequest(conn); err != nil {
				return
			}
			var frame []byte
			switch responses.Add(1) {
			case 1:
				frame = testResponse(StatusOK, []byte{0, 255, 1, 2})
			case 2:
				frame = testResponse(StatusOK, nil)
			default:
				frame = testResponse(StatusNotFound, nil)
			}
			for _, b := range frame {
				if _, err := conn.Write([]byte{b}); err != nil {
					return
				}
			}
		}
	})
	value, err := client.Get(context.Background(), []byte("key"))
	if err != nil || !bytes.Equal(value, []byte{0, 255, 1, 2}) {
		t.Fatalf("value=%v error=%v", value, err)
	}
	value, err = client.Get(context.Background(), []byte("empty"))
	if err != nil || value == nil || len(value) != 0 {
		t.Fatalf("empty value=%v error=%v", value, err)
	}
	value, err = client.Get(context.Background(), []byte("missing"))
	if value != nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("miss value=%v error=%v", value, err)
	}
}

func TestServerErrorsPreserveStatusAndMutationCertainty(t *testing.T) {
	for _, test := range []struct {
		status           Status
		sentinel         error
		unknown, discard bool
	}{
		{StatusBusy, ErrBusy, false, false},
		{StatusCapacity, ErrCapacity, false, false},
		{StatusInvalidRequest, ErrInvalidRequest, false, true},
		{StatusStorageError, ErrStorage, true, false},
		{StatusUnavailable, ErrUnavailable, true, false},
		{StatusTimedOut, ErrTimedOut, true, true},
	} {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
				for {
					if _, err := readTestRequest(conn); err != nil {
						return
					}
					if _, err := conn.Write(testResponse(test.status, []byte("diagnostic"))); err != nil {
						return
					}
				}
			})
			err := client.Put(context.Background(), []byte("key"), []byte("value"))
			var server *ServerError
			if !errors.Is(err, test.sentinel) || !errors.As(err, &server) || server.Status != test.status || server.Message != "diagnostic" {
				t.Fatalf("wrong server error: %v", err)
			}
			if errors.Is(err, ErrOutcomeUnknown) != test.unknown {
				t.Fatalf("wrong certainty: %v", err)
			}
			if dials.Load() != 1 {
				t.Fatal("request was retried automatically")
			}
			_, _ = client.Get(context.Background(), []byte("key"))
			wantDials := int32(1)
			if test.discard {
				wantDials = 2
			}
			if dials.Load() != wantDials {
				t.Fatalf("dials=%d want=%d", dials.Load(), wantDials)
			}
		})
	}
}

func TestInvalidResponsesDiscardConnectionWithoutRetry(t *testing.T) {
	base := testResponse(StatusOK, nil)
	badMagic := bytes.Clone(base)
	badMagic[0] = 'X'
	badReserved := bytes.Clone(base)
	badReserved[5] = 1
	badStatus := bytes.Clone(base)
	badStatus[4] = 255
	tooLarge := bytes.Clone(base)
	binary.LittleEndian.PutUint32(tooLarge[8:], math.MaxUint32)
	badMiss := testResponse(StatusNotFound, []byte("x"))
	badError := testResponse(StatusBusy, nil)
	binary.LittleEndian.PutUint32(badError[8:], maxErrorBytes+1)
	truncatedBody := testResponse(StatusOK, []byte("abc"))
	truncatedBody = truncatedBody[:len(truncatedBody)-1]
	for name, frame := range map[string][]byte{"magic": badMagic, "reserved": badReserved, "status": badStatus, "size": tooLarge, "miss": badMiss, "error_size": badError, "short_header": base[:7], "short_body": truncatedBody} {
		t.Run(name, func(t *testing.T) {
			var attempts atomic.Int32
			client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
				if _, err := readTestRequest(conn); err != nil {
					return
				}
				response := frame
				if attempts.Add(1) > 1 {
					response = testResponse(StatusOK, []byte("fresh"))
				}
				_, _ = conn.Write(response)
			})
			if _, err := client.Get(context.Background(), []byte("key")); err == nil {
				t.Fatal("invalid frame accepted")
			}
			if dials.Load() != 1 {
				t.Fatal("failed request retried")
			}
			value, err := client.Get(context.Background(), []byte("key"))
			if err != nil || string(value) != "fresh" || dials.Load() != 2 {
				t.Fatalf("fresh call failed: %q %v", value, err)
			}
		})
	}
}

func TestMutationRejectsUnexpectedResponseBody(t *testing.T) {
	client, _ := pipeClient(t, Options{}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err == nil {
			_, _ = conn.Write(testResponse(StatusOK, []byte("unexpected")))
		}
	})
	err := client.Put(context.Background(), []byte("key"), []byte("value"))
	if !errors.Is(err, ErrProtocol) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
}

func TestValidationDoesNotUseConnection(t *testing.T) {
	client, dials := pipeClient(t, Options{MaxKeyBytes: 3, MaxValueBytes: 3}, func(net.Conn) { t.Error("invalid call reached network") })
	for _, call := range []func() error{
		func() error { _, err := client.Get(context.Background(), nil); return err },
		func() error { return client.Put(context.Background(), []byte("long"), nil) },
		func() error { return client.Put(context.Background(), []byte("key"), []byte("long")) },
		func() error { return client.Put(context.Background(), []byte("key"), nil, WithTTL(-1)) },
		func() error { return client.Delete(context.Background(), []byte("key"), WithTTL(1)) },
		func() error { return client.Put(context.Background(), []byte("key"), nil, nil) },
		func() error { _, err := client.Get(nil, []byte("key")); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("error=%v", err)
		}
	}
	if dials.Load() != 0 {
		t.Fatalf("validation dialed %d times", dials.Load())
	}
}

func TestCancellationInterruptsInFlightIOAndPreservesUnknownOutcome(t *testing.T) {
	received, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err != nil {
			return
		}
		close(received)
		<-release
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- client.Put(ctx, []byte("key"), []byte("value")) }()
	awaitSignal(t, received)
	cancel()
	err := waitError(t, result)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
	if dials.Load() != 1 {
		t.Fatal("canceled operation was retried")
	}
}

func TestPoolWaitHonorsContextWithoutSendingMutation(t *testing.T) {
	received, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err != nil {
			return
		}
		close(received)
		<-release
	})
	active := make(chan error, 1)
	go func() { _, err := client.Get(context.Background(), []byte("occupied")); active <- err }()
	awaitSignal(t, received)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := client.Put(ctx, []byte("waiting"), []byte("value"))
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
	if dials.Load() != 1 {
		t.Fatal("pool bound exceeded")
	}
	_ = client.Close()
	if err := waitError(t, active); !errors.Is(err, ErrClosed) {
		t.Fatalf("active error=%v", err)
	}
}

func TestConfiguredTimeoutBoundsResponseWait(t *testing.T) {
	client, _ := pipeClient(t, Options{Timeout: 20 * time.Millisecond}, func(conn net.Conn) {
		if _, err := readTestRequest(conn); err != nil {
			return
		}
		var byte [1]byte
		_, _ = conn.Read(byte[:]) // Wait until the client discards its timed-out socket.
	})
	err := client.Put(context.Background(), []byte("key"), []byte("value"))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
}

func TestConcurrentCallsRespectPoolBoundAndMatchResponses(t *testing.T) {
	ready := make(chan struct{}, 2)
	gate := make(chan struct{})
	client, dials := pipeClient(t, Options{PoolSize: 2}, func(conn net.Conn) {
		first := true
		for {
			request, err := readTestRequest(conn)
			if err != nil {
				return
			}
			if first {
				ready <- struct{}{}
				first = false
				<-gate
			}
			if _, err := conn.Write(testResponse(StatusOK, request.key)); err != nil {
				return
			}
		}
	})
	var callers sync.WaitGroup
	for id := 0; id < 40; id++ {
		callers.Add(1)
		go func(id int) {
			defer callers.Done()
			key := []byte(fmt.Sprintf("key-%d", id))
			value, err := client.Get(context.Background(), key)
			if err != nil || !bytes.Equal(value, key) {
				t.Errorf("key=%q value=%q error=%v", key, value, err)
			}
		}(id)
	}
	awaitSignal(t, ready)
	awaitSignal(t, ready)
	close(gate)
	callers.Wait()
	if dials.Load() != 2 {
		t.Fatalf("dials=%d, want 2", dials.Load())
	}
}

func TestCloseCancelsDialAndIsIdempotent(t *testing.T) {
	client, err := NewClient("127.0.0.1:7379", Options{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	client.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	result := make(chan error, 1)
	go func() { result <- client.Put(context.Background(), []byte("key"), []byte("value")) }()
	awaitSignal(t, started)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	err = waitError(t, result)
	if !errors.Is(err, ErrClosed) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
	_, err = client.Get(context.Background(), []byte("key"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("closed client error=%v", err)
	}
}

func TestIdleConnectionExpiresBeforeNextRequest(t *testing.T) {
	client, dials := pipeClient(t, Options{PoolSize: 1}, func(conn net.Conn) {
		for {
			if _, err := readTestRequest(conn); err != nil {
				return
			}
			if _, err := conn.Write(testResponse(StatusOK, nil)); err != nil {
				return
			}
		}
	})
	if _, err := client.Get(context.Background(), []byte("key")); err != nil {
		t.Fatal(err)
	}
	pc := <-client.slots
	pc.lastUsed = time.Now().Add(-time.Minute)
	client.slots <- pc
	if _, err := client.Get(context.Background(), []byte("key")); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 {
		t.Fatalf("idle socket reused: %d dials", dials.Load())
	}
}

func TestNewClientRejectsInvalidConfiguration(t *testing.T) {
	for _, address := range []string{"", "localhost", ":7379", "localhost:0", "localhost:no", "localhost:65536"} {
		if _, err := NewClient(address, Options{}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("address=%q error=%v", address, err)
		}
	}
	for _, options := range []Options{{PoolSize: -1}, {PoolSize: 1025}, {Timeout: -1}, {IdleTimeout: -1}, {MaxKeyBytes: 65537}, {MaxValueBytes: 64*1024*1024 + 1}} {
		if _, err := NewClient("localhost:7379", options); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("options=%+v error=%v", options, err)
		}
	}
}
