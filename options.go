package smkv

import (
	"fmt"
	"time"
)

// Options configures a Client. Zero fields select the documented defaults.
// Limits must also fit the server's configuration; there is no negotiation yet.
type Options struct {
	Database string // Empty selects default; immutable for this client.
	// Independent data and management security; zero values preserve plaintext.
	Security           SecurityOptions
	ManagementSecurity SecurityOptions
	PoolSize           int           // Default 4; at most 1,024 concurrent connections.
	Timeout            time.Duration // Default 5s; total operation budget, including pool wait.
	IdleTimeout        time.Duration // Default 5s; discard idle sockets before their next use.
	MaxKeyBytes        int           // Default 1 KiB; maximum 64 KiB.
	MaxValueBytes      int           // Default 1 MiB; maximum 64 MiB.
	// Keys per batch get request; larger batches are split. Default 5,000;
	// at most 100,000.
	MaxBatchKeys int
	// Largest batch get reply accepted. Default 64 MiB; 1 KiB to 1 GiB.
	MaxBatchResponseBytes int
}

func (o Options) defaults() (Options, error) {
	if o.Database == "" {
		o.Database = "default"
	}
	if len(o.Database) > 64 {
		return o, fmt.Errorf("%w: invalid database", ErrInvalidArgument)
	}
	for _, c := range o.Database {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return o, fmt.Errorf("%w: invalid database", ErrInvalidArgument)
		}
	}
	var err error
	o.Security, err = o.Security.validated()
	if err != nil {
		return o, err
	}
	o.ManagementSecurity, err = o.ManagementSecurity.validated()
	if err != nil {
		return o, err
	}
	if o.PoolSize == 0 {
		o.PoolSize = 4
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 5 * time.Second
	}
	if o.MaxKeyBytes == 0 {
		o.MaxKeyBytes = 1024
	}
	if o.MaxValueBytes == 0 {
		o.MaxValueBytes = 1024 * 1024
	}
	if o.MaxBatchKeys == 0 {
		o.MaxBatchKeys = 5000
	}
	if o.MaxBatchResponseBytes == 0 {
		o.MaxBatchResponseBytes = 64 * 1024 * 1024
	}
	if o.PoolSize < 1 || o.PoolSize > 1024 || o.Timeout < 0 || o.IdleTimeout < 0 ||
		o.MaxKeyBytes < 1 || o.MaxKeyBytes > 64*1024 || o.MaxValueBytes < 1 || o.MaxValueBytes > 64*1024*1024 ||
		o.MaxBatchKeys < 1 || o.MaxBatchKeys > 100000 || o.MaxBatchResponseBytes < 1024 || o.MaxBatchResponseBytes > 1<<30 {
		return o, fmt.Errorf("%w: invalid pool size, timeout, or size limit", ErrInvalidArgument)
	}
	return o, nil
}

type writeOptions struct {
	database bool
	// Report the record's version; with expect, apply only at `expected`
	// (0: absent). See docs/proposals/generation-cas.md.
	report            bool
	expect            bool
	expected          uint64
	ttlSet            bool
	ttlMS             uint64
	sync              bool
	replicaAck        bool
	durableReplicaAck bool
}

// WriteOption configures an individual Put or Delete.
type WriteOption func(*writeOptions) error

// WithTTL sets a Put's expiration. Zero expires immediately; negative durations
// are rejected. Positive fractional milliseconds round up. Delete rejects TTLs.
func WithTTL(ttl time.Duration) WriteOption {
	return func(o *writeOptions) error {
		if ttl < 0 {
			return fmt.Errorf("%w: TTL must not be negative", ErrInvalidArgument)
		}
		o.ttlSet = true
		o.ttlMS = uint64(ttl / time.Millisecond)
		if ttl%time.Millisecond != 0 {
			o.ttlMS++
		}
		return nil
	}
}

// WithSync asks the server to sync the affected partition before acknowledging
// the mutation. It does not add replication or cluster durability.
func WithSync() WriteOption {
	return func(o *writeOptions) error { o.sync = true; return nil }
}

func parseWriteOptions(options []WriteOption) (writeOptions, error) {
	var parsed writeOptions
	for _, option := range options {
		if option == nil {
			return parsed, fmt.Errorf("%w: nil write option", ErrInvalidArgument)
		}
		if err := option(&parsed); err != nil {
			return parsed, err
		}
	}
	return parsed, nil
}

// WithReplicaAck requires the current replica to apply the mutation before success.
// The primary is also synced. Requires a quorum-managed RF2 cluster.
// Failures after admission may wrap ErrOutcomeUnknown and must not be retried blindly.
func WithReplicaAck() WriteOption {
	return func(o *writeOptions) error { o.replicaAck = true; return nil }
}

// WithDurableReplicaAck syncs both the primary and current replica before success.
// Requires a quorum-managed RF2 cluster; unavailable replicas prevent success.
func WithDurableReplicaAck() WriteOption {
	return func(o *writeOptions) error { o.replicaAck = true; o.durableReplicaAck = true; return nil }
}
