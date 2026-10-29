package smkv

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestSecurityRejectsDisabledTLSVerification(t *testing.T) {
	for _, cfg := range []*tls.Config{{InsecureSkipVerify: true}, {MinVersion: tls.VersionTLS10}} {
		if _, err := NewClient("localhost:7379", Options{Security: SecurityOptions{TLS: cfg}}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("expected configuration rejection: %v", err)
		}
	}
}
func TestAuthenticationFailureDoesNotSendMutation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		c, e := listener.Accept()
		if e != nil {
			done <- e
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		header := make([]byte, 6)
		_, e = io.ReadFull(c, header)
		if e == nil && string(header[:4]) != "SKA1" {
			e = errors.New("missing authentication preamble")
		}
		if e == nil {
			_, e = io.CopyN(io.Discard, c, int64(binary.LittleEndian.Uint16(header[4:])))
		}
		if e == nil {
			_, e = c.Write([]byte{1})
		}
		if e == nil {
			var b [1]byte
			_, e = c.Read(b[:])
			if errors.Is(e, io.EOF) {
				e = nil
			} else {
				e = errors.New("client sent data after authentication failure")
			}
		}
		done <- e
	}()
	client, err := NewClient(listener.Addr().String(), Options{Security: SecurityOptions{Credentials: func(context.Context) (Credential, error) {
		return Credential{Name: "alice", Token: "0123456789abcdef0123456789abcdef"}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Put(context.Background(), []byte("key"), []byte("value"))
	if !errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("authentication should fail before mutation: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
