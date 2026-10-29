// Package smkv provides a concurrent TCP client for the SMKV v1 protocol.
//
// A Client maintains a bounded, lazily connected pool. Contexts and the configured
// timeout bound pool acquisition, dialing, writing, and reading together. Calls
// never retry automatically. Errors matching ErrOutcomeUnknown mean a mutation
// may have completed; blindly retrying can change its value or expiration.
//
// The raw API uses opaque byte keys and values. Table handles add typed columns,
// projected reads, and atomic updates; Update preserves TTL when omitted.
// Raw Get distinguishes a missing key (ErrNotFound) from a present empty value
// (an empty slice and nil error). WithTTL(0) expires immediately. Omitted TTL on
// raw Put or Table.Put means no expiration.
package smkv
