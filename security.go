package smkv

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Credential identifies a client. Keep Token out of logs and command-line arguments.
type Credential struct {
	Name  string `json:"name"`
	Token string `json:"token"`
}

// SecurityOptions independently enables TLS and authentication. Credentials is called
// for each new socket, supporting token rotation without rebuilding the client.
// Existing sockets are reauthorized by the server; rejected requests are not retried.
type SecurityOptions struct {
	TLS         *tls.Config
	Credentials func(context.Context) (Credential, error)
}

func (s SecurityOptions) validated() (SecurityOptions, error) {
	if s.TLS != nil {
		s.TLS = s.TLS.Clone()
		if s.TLS.InsecureSkipVerify {
			return s, fmt.Errorf("%w: TLS verification cannot be disabled", ErrInvalidArgument)
		}
		if s.TLS.MinVersion == 0 {
			s.TLS.MinVersion = tls.VersionTLS12
		}
		if s.TLS.MinVersion < tls.VersionTLS12 {
			return s, fmt.Errorf("%w: TLS 1.2 minimum required", ErrInvalidArgument)
		}
	}
	return s, nil
}

// CredentialFile reads credentials on each new connection. Replace the file atomically
// with owner-only permissions when rotating. No secret values are returned in errors.
func CredentialFile(path string) func(context.Context) (Credential, error) {
	return func(ctx context.Context) (Credential, error) {
		var c Credential
		if err := ctx.Err(); err != nil {
			return c, err
		}
		f, err := os.Open(path)
		if err != nil {
			return c, err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, 4097))
		if err != nil {
			return c, err
		}
		if len(b) > 4096 || json.Unmarshal(b, &c) != nil {
			return Credential{}, fmt.Errorf("%w: invalid credential file", ErrInvalidArgument)
		}
		return c, nil
	}
}
func (s SecurityOptions) dial(ctx context.Context, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = conn.Close()
		}
	}()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Second)
	}
	_ = conn.SetDeadline(deadline)
	raw := conn
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Now()); close(stopped) })
	defer func() {
		if !stop() {
			<-stopped
		}
	}()
	if s.TLS != nil {
		cfg := s.TLS.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName, _, err = net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
		}
		tlsConn := tls.Client(conn, cfg)
		if err = tlsConn.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		conn = tlsConn
	}
	if s.Credentials != nil {
		c, e := s.Credentials(ctx)
		if e != nil {
			return nil, e
		}
		valid := len(c.Name) > 0 && len(c.Name) <= 64 && len(c.Token) >= 32 && len(c.Token) <= 256
		for _, b := range []byte(c.Name) {
			if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.') {
				valid = false
			}
		}
		for _, b := range []byte(c.Token) {
			if b < 33 || b > 126 {
				valid = false
			}
		}
		if !valid {
			return nil, fmt.Errorf("%w: invalid credential format", ErrInvalidArgument)
		}
		b, _ := json.Marshal(c)
		header := []byte{'S', 'K', 'A', '1', 0, 0}
		binary.LittleEndian.PutUint16(header[4:], uint16(len(b)))
		if _, err = (&net.Buffers{header, b}).WriteTo(conn); err != nil {
			return nil, err
		}
		var status [1]byte
		if _, err = io.ReadFull(conn, status[:]); err != nil {
			return nil, err
		}
		if status[0] != 0 {
			return nil, ErrUnauthorized
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return conn, nil
}

// LoadSecurityConfig loads data and management profiles from the Rust tools' JSON
// config format. Relative filenames resolve beside the config. TLS CA changes
// require a new Client; credential files are read on each new connection.
func LoadSecurityConfig(path string) (SecurityOptions, SecurityOptions, error) {
	type profile struct {
		TLS         bool   `json:"tls"`
		CA          string `json:"ca"`
		Credential  string `json:"credential"`
		Certificate string `json:"certificate"`
		PrivateKey  string `json:"private_key"`
		Users       string `json:"users"`
	}
	var config struct {
		Data        profile `json:"data"`
		Management  profile `json:"management"`
		Replication profile `json:"replication"`
	}
	f, err := os.Open(path)
	if err != nil {
		return SecurityOptions{}, SecurityOptions{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return SecurityOptions{}, SecurityOptions{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if len(b) > 1<<20 || decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return SecurityOptions{}, SecurityOptions{}, fmt.Errorf("%w: invalid security config", ErrInvalidArgument)
	}
	resolve := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(filepath.Dir(path), p)
	}
	load := func(p profile) (SecurityOptions, error) {
		var s SecurityOptions
		if p.TLS {
			if p.CA == "" {
				return s, fmt.Errorf("%w: TLS client CA required", ErrInvalidArgument)
			}
			pem, err := os.ReadFile(resolve(p.CA))
			if err != nil {
				return s, err
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return s, fmt.Errorf("%w: invalid TLS CA", ErrInvalidArgument)
			}
			s.TLS = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		} else if p.CA != "" || p.Certificate != "" || p.PrivateKey != "" {
			return s, fmt.Errorf("%w: TLS files require tls=true", ErrInvalidArgument)
		}
		if p.Credential != "" {
			s.Credentials = CredentialFile(resolve(p.Credential))
		}
		return s, nil
	}
	data, err := load(config.Data)
	if err != nil {
		return data, SecurityOptions{}, err
	}
	management, err := load(config.Management)
	return data, management, err
}
