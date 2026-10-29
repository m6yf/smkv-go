# smkv-go

The Go client for SMKV, a distributed key-value database. It needs Go 1.22
or later and only the standard library. It supports binary keys and values,
tables with typed columns, TTLs, sync-on-write, replicated acknowledgments,
batch reads, scans, TLS and authentication, contexts, and a bounded connection
pool safe for concurrent goroutines. `ClusterClient` discovers a cluster and
routes each key to its partition's owner.

The [`aerospikeshadow`](aerospikeshadow) module runs SMKV beside Aerospike as a
shadow: an application keeps its Aerospike client, and its writes are copied to
SMKV while a sample of its reads is compared.

## Install

```sh
go get github.com/m6yf/smkv-go
```

## Use

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "time"

    smkv "github.com/m6yf/smkv-go"
)

func main() {
    client, err := smkv.NewClient("127.0.0.1:7379", smkv.Options{
        PoolSize: 4,
        Timeout:  5 * time.Second,
    })
    if err != nil { log.Fatal(err) }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    key := []byte("greeting")
    err = client.Put(ctx, key, []byte("hello"), smkv.WithTTL(time.Minute))
    if err != nil { log.Fatal(err) }

    value, err := client.Get(ctx, key)
    if errors.Is(err, smkv.ErrNotFound) { fmt.Println("cache miss"); return }
    if err != nil { log.Fatal(err) }
    fmt.Println(string(value))

    if err := client.Delete(ctx, key, smkv.WithSync()); err != nil { log.Fatal(err) }
}
```

With a node running, `go run ./examples/basic -addr host:port` runs this.

## Behavior

- Connections are established lazily. A client targets one node; the server routes
  keys to its local partitions. Use `ClusterClient` below for cluster discovery and
  owner routing.
- With fixed-role replication, direct mutations to a primary client and use a
  separate replica client for stale reads. Replica writes return `ErrInvalidRequest`;
  reads before initial history validation return `ErrUnavailable`.
- Each connection handles one complete request/response at a time. Pool capacity
  bounds active requests; callers wait with their context. Await writes before
  dependent reads; concurrent calls across connections have no ordering guarantee.
- `Timeout` bounds the entire call, including waiting for a pool slot, dialing, and
  I/O. An earlier context deadline wins. Cancellation interrupts blocked socket I/O.
- Omit `WithTTL` for no expiration. `WithTTL(0)` expires immediately. Positive
  fractional milliseconds round up. Negative TTLs and TTLs on Delete are rejected.
- Empty values are valid. Missing or expired keys return `ErrNotFound`. Delete of
  an absent key succeeds. `WithSync()` waits for the affected partition's local
  sync, not replication.
- Direct `Client` calls never retry automatically. A failed connection is discarded; a later call
  may open a new connection. A server can close an idle connection sooner than the
  SDK expects, so even reads can encounter a transport error on first reuse.
- `GetVersioned`, `PutIf` and `DeleteIf` read and check record versions
  (Aerospike's generation check) on device-engine servers. `PutIf` with
  version 0 writes only if the key is absent. A mismatch returns
  `ErrConflict` and writes nothing.
- `BatchGet` reads many keys in one request per `MaxBatchKeys` keys (default
  5,000), as Aerospike's batch read. Values come back in request order, `nil` for
  a miss and non-nil for a stored empty value. Keys the server defers (`Busy`, past
  its reply cap) are resent within the deadline; any other per-key failure fails
  the call. Against a server without batch get, the client uses single gets.
- `Close` is idempotent and interrupts active operations, pool waits, and dialing.
  Returned values belong to the caller. Do not modify input slices during a call.

Defaults: 4 connections, 5-second operation timeout, 5-second idle socket lifetime,
1 KiB keys, 1 MiB values, 5,000 keys per batch request and 64 MiB batch replies. Zero option fields select defaults. Configure limits
to match the server; there is no negotiation. Keep `IdleTimeout` shorter than the
server's read timeout. Oversized response headers are rejected before allocation.
TLS and authentication are optional; see the security configuration section below.

## Tables and columns

Both `Client` and `ClusterClient` expose lightweight table handles:

```go
users := client.Table("users")
err := users.Put(ctx, []byte("12345"), smkv.Columns{
    "country": "US",
    "visits": int64(12),
    "active": true,
}, smkv.WithTTL(time.Hour))
if err != nil { return err }

// Atomically modify selected columns, preserving the others and the row TTL.
if err := users.Update(ctx, []byte("12345"),
    smkv.Columns{"visits": int64(13)}, []string{"active"}); err != nil {
    return err
}
row, err := users.Get(ctx, []byte("12345"), "country", "visits")

// Atomically add to counters, creating the row or column if missing; the
// reply holds the new values.
counts, err := users.Increment(ctx, []byte("12345"), smkv.Columns{"visits": 1, "score": 0.5})
```

Columns support `[]byte`, UTF-8 `string`, `int64` (also `int` on input), `float64`,
and `bool`. Integers decode as `int64`. Tables and columns have 1–64-byte UTF-8
names; rows contain at most 64 columns. Different rows can have different columns
and types. Empty rows and empty byte/string values are valid. There is no null
type; use Update's remove list to delete a column. Do not modify input maps or
slices until the call returns.

`Get` with no projection returns all columns. Missing projected columns are
omitted; a missing row returns `ErrNotFound`. `Put` replaces the entire row and
omitted TTL means no expiration. `Update` requires an existing, unexpired row;
omitted TTL preserves its absolute expiration. `WithTTL` resets the row TTL,
and zero expires it immediately. Clearing expiration requires replacing the row
with `Put` without TTL. `Delete` is idempotent and accepts only `WithSync`.
`GetReplica` on cluster table handles explicitly allows stale replica reads.
`Increment` adds `int`/`int64` amounts to integer columns and `float64` amounts
to double columns. A missing row or column starts at the amount. Overflow, or a
column holding another type, returns `ErrInvalidRequest` and writes nothing.
Omitted TTL keeps the row's expiration. An increment is not idempotent: an
`OutcomeUnknown` error may have applied and is never retried.

The encoded table name and key count against `MaxKeyBytes`; column metadata and
values count against `MaxValueBytes`. Raw byte keys and table rows occupy distinct
storage domains. Tables are distributed over existing partitions with no separate
catalog. Projection saves response bytes but still reads the complete disk row;
updates rewrite the complete row. Whole-row reads forward its stored encoding.

**Upgrade every server before writing rows.** Old raw data remains readable, but
older servers cannot replay row records.

## Errors

Use `errors.Is` for `ErrNotFound`, `ErrBusy`, `ErrInvalidArgument`,
`ErrInvalidRequest`, `ErrStorage`, `ErrUnavailable`, `ErrTimedOut`, `ErrClosed`,
and `ErrProtocol`. Context cancellations and deadlines remain detectable with
`errors.Is(err, context.Canceled)` and `errors.Is(err, context.DeadlineExceeded)`.
Use `errors.As` to inspect `*smkv.ServerError` or `*smkv.OpError`.

For mutations, `errors.Is(err, smkv.ErrOutcomeUnknown)` means the operation may
have completed. This includes transport/protocol failures after writing began and
conservatively includes server storage errors, unavailability, and execution
timeouts. Do not blindly retry these failures. A Busy response means the server
did not accept the request. Failed validation, dialing, and pool waits do not mark
the outcome unknown. None of these errors trigger automatic retries.

## Sharded clusters

Seed addresses use the private administration port, not the data port:

```go
cluster, err := smkv.NewClusterClient(ctx, []string{"192.0.2.10:7381", "192.0.2.11:7381"}, smkv.Options{})
if err != nil { return err }
defer cluster.Close()
err = cluster.Put(ctx, []byte("key"), []byte("value"))
value, err := cluster.Get(ctx, []byte("key"))
```

The client hashes opaque keys with IEEE CRC32, routes to each partition's primary,
and refreshes the versioned map on explicit `ErrWrongOwner` rejection. `GetReplica`
opts into stale reads. An uncertain mutation is never automatically retried.
Mutation `Busy` responses also return to the caller. Cluster reads retry transient
transport, Busy, Unavailable and execution-timeout failures with a map refresh.
Read attempts are capped at 500 ms (or half Options.Timeout), with 10–100 ms
exponential backoff. NotFound, protocol/storage errors and capacity failures are
terminal. Primary reads never silently fall back to a replica. Refresh,
connection acquisition, and request execution share the caller's deadline and
configured timeout. `Map()` returns a detached copy for diagnostics. `BatchGet` groups keys by
owning node, sends one batch per node concurrently, and retries keys answered
`WrongOwner`, `Busy`, `Unavailable` or `TimedOut` after a map refresh.

Capacity admission rejections are exposed as `ErrCapacity` (TCP status 8). Use
`errors.Is(err, smkv.ErrCapacity)` to distinguish a known rejected mutation from
an uncertain storage or transport outcome. Inspect resource headroom before retrying;
the SDK does not automatically retry these writes. Upgrade clients before servers.

After a Busy/unavailable response or transport failure, `ClusterClient` marks its map
for refresh before the next operation. It queries discovered admin endpoints in
parallel with bounded probe deadlines and adopts the highest valid epoch. A stale
or stalled initial seed cannot prevent discovery of a reachable promoted owner.
The failed mutation is returned with its existing outcome classification; unknown
mutations are never automatically retried. `Refresh(ctx)` remains available for
explicit operator/application control.

## Replicated acknowledgments

For quorum-managed RF2 clusters, opt in per mutation:

```go
err := client.Put(ctx, key, value, smkv.WithReplicaAck())
err = client.Put(ctx, key, value, smkv.WithDurableReplicaAck())
```

`WithReplicaAck()` syncs the primary and waits for the current replica to apply.
`WithDurableReplicaAck()` syncs both copies. These options also apply to deletes
and table puts, updates and deletes. Async remains the default; `WithSync()` alone
still means local durability only. Upgrade both owners before opting in: older
servers reject the new flags. Non-quorum deployments reject without mutation.

The server admits at most eight concurrent confirmed mutations per node; excess
requests receive `ErrBusy` before mutation. The confirmation deadline is five seconds; choose a client timeout above
that to receive its diagnostic, or use a shorter deadline intentionally.
After admission, errors can wrap `ErrOutcomeUnknown`; neither the direct nor
cluster client automatically retries these writes. Confirmation protects against
a single node failure with the other copy healthy, subject to ordinary later
updates, deletes and TTL.

## Optional TLS and authentication

Zero security options preserve the plaintext LAN protocol. To load the same JSON
profiles used by SMKV tools:

```go
data, management, err := smkv.LoadSecurityConfig("client.json")
if err != nil { return err }
client, err := smkv.NewClusterClient(ctx, seeds, smkv.Options{
    Security: data, ManagementSecurity: management,
})
```

Alternatively set `SecurityOptions.TLS` to a verified `*tls.Config` and
`Credentials` to `smkv.CredentialFile(path)` or your credential callback. TLS and
authentication are independent. Discovery uses `ManagementSecurity`; data sockets
use `Security`. There is no plaintext or anonymous fallback. Authentication-only
mode transmits bearer credentials in clear and is intended only for trusted LANs.
`ErrUnauthorized` is a definite rejection, not `ErrOutcomeUnknown`. Rotation takes
effect on new sockets; revoked existing sessions are denied by the server.

## Table scans

Cluster scans page through primaries without a cluster-wide snapshot. They can
return empty pages with a continuation cursor. Concurrent mutations may cause
omissions or duplicates; topology changes, compaction, and restart can require
restarting. No automatic scan retries are performed.

```go
options := smkv.ScanOptions{
    Limit: 50,
    Projection: []string{"name", "age"},
    Filters: []smkv.ScanFilter{{Column: "age", Op: "ge", Value: int64(18)}},
}
page, err := cluster.Table("users").Scan(ctx, options)
// Handle err; Busy permits retrying this page after 100 ms.
// Process page.Rows, then set options.Cursor = page.Cursor.
// An empty page.Cursor means complete; an empty page.Rows does not.
```

`ScanFilter.Op` supports `eq`, `lt`, `le`, `gt`, `ge`, and `exists` (nil value).
Comparisons are type-specific; integers and floats are not coerced. Multiple
filters are ANDed. Bytes, strings, int64, float64 and bool decode into the existing
`Columns` types. For standalone nodes use `client.Table("users").ScanPartition(ctx,
partition, options)` and explicitly visit each configured partition. Standalone
partition cursors and cluster cursors are distinct and are not interchangeable
with CLI cursors. Callers should enforce an overall deadline/record budget.

Scans run below foreground storage work and can time out under sustained load.
For background scanning, use a separate client so scan requests do not occupy
connections needed by your foreground reads and writes.

## Logical databases

Select the database when constructing the client (or cluster client):

```go
smkv.Options{Database: "orders"}
```

Omitting the option uses `default` and preserves existing wire keys. Named
databases require database-aware development servers (cluster protocol 5, or 6
with experimental group sync). Names use 1–64 ASCII letters, digits, `_` or `-`.
The selection scopes raw keys and all table operations, including replica reads.
The complete database-prefixed key is hashed for routing and counts against the
key-size limit. Databases share cluster capacity and durability settings.

## Tests

```sh
go test -race ./...
go vet ./...
```

The tests need no server: they run against in-process fakes and the protocol's
test vectors in `testdata/`.

## License

[Apache License 2.0](LICENSE).
