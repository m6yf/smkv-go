# aerospikeshadow

Runs SMKV beside Aerospike as a shadow. The application keeps its Aerospike
client, wrapped: every write Aerospike accepts is also sent to SMKV, and a
sample of reads is compared with SMKV's copy. Aerospike stays the only source
of answers.

```sh
go get github.com/m6yf/smkv-go/aerospikeshadow
```

```go
shadow, err := aerospikeshadow.New(aerospikeshadow.Config{
    Seeds:      []string{"192.0.2.10:7381"}, // none: a nil shadow, which copies nothing
    Namespace:  "app",
    ReadSample: 0.01,
    OffFile:    "/etc/myapp/smkv-off",
    Count:      func(event string) { metrics.Incr("smkv_shadow." + event) },
})
if err != nil { return err }

v8, err := as.NewClientWithPolicyAndHost(policy, hosts...) // aerospike-client-go/v8
if err != nil { return err }
client := aerospikeshadow.WrapV8(v8, shadow) // or WrapV4 for aerospike-client-go v4
```

`ClientV4` and `ClientV8` embed the Aerospike clients, so code that calls
`client.Put`, `client.Get` or `client.ScanAll` keeps working unchanged. `Put`,
`PutBins`, `Delete`, `Get` and the v8 `BatchGet` are shadowed; everything else,
scans included, goes to Aerospike alone.

## What reaches SMKV

- Only writes and deletes Aerospike accepted. A refused write returns
  Aerospike's error unchanged and is not copied.
- With Aerospike's meaning: `UPDATE` (the default) sets the bins written and
  creates a missing row; `REPLACE`, `REPLACE_ONLY` and `CREATE_ONLY` replace the
  row; a nil bin removes its column.
- In the order Aerospike accepted them, per key: a key's copies always take the
  same lane.
- Addressed as SMKV's migration tool converts an Aerospike export, so a shadow
  can start from a bulk load: the namespace is the SMKV database in
  `Config.Options`, a set is the table of the same name (records without a set
  go to `Config.UnsetTable`, `unset`), and a key is a string's UTF-8 bytes, a
  blob's bytes or an integer's 8 bytes little endian.

Strings, blobs, integers, doubles and booleans copy. A write with a list, map
or other bin SMKV has no column for is not copied, and counted `unsupported`.

## Effect on the application

- Copies are asynchronous; when a lane's queue is full, new copies are dropped,
  never waited for.
- SMKV's errors never reach the caller. After 50 failures in a row, copies pause
  for 10 s.
- Until SMKV is reachable, the shadow drops copies and keeps retrying in the
  background.
- While `Config.OffFile` exists, nothing is sent; it is checked every second.
- Memory is bounded by `Lanes × Queue` queued writes.

## Reads

A sampled read is compared after the copies queued before it. A mismatch gets a
second look after `Config.Recheck` (2 s), so a copy still in flight is counted
`converged`, not as a mismatch. Only the bins the read named are compared.

`Config.Count` receives one event per copy and comparison: `merge`, `replace`,
`delete` (and their `_error` and `_invalid` forms), `match`, `converged`,
`mismatch_missing`, `mismatch_extra`, `mismatch_value`, `compare_error`,
`dropped`, `paused`, `not_connected`, `no_user_key`, `unsupported` and
`other_namespace`. At most one mismatch every 10 s is logged, with its key
hashed.

## Tests

```sh
go test -race ./...
AEROSPIKE_ADDR=127.0.0.1:3000 SMKV_SEEDS=127.0.0.1:7381 go test -race -tags=integration ./...
```

The integration tests write to a disposable Aerospike (namespace `test`, or
`AEROSPIKE_NAMESPACE`) and SMKV cluster with both clients, and check each
row SMKV holds against Aerospike's record.
