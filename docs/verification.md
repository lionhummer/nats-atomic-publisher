# 0.1.0 verification

The release suite contains 18 injected-backend regression tests, five explicitly enabled dedicated-broker tests, and one compiled README example. Three executable examples provision and delete their own uniquely named streams.

## What is checked

- Coalescing, individual receipt ranges, non-overlap, exclusive comparisons, and batch message limits.
- 100 concurrent two-message submissions with complete, unique sequence coverage.
- Stale-group isolation and a concurrent change between subject inspection and retry.
- Failed and timed-out inspection without replay.
- Cancellation before admission, while waiting for bytes, and after transport starts.
- Budget recovery, ordinary saturation, priority scheduling, and priority policy bypass.
- Indeterminate/invalid receipts without automatic retries.
- Invalid headers, subjects, stream expectations, comparison shape, and extreme timer values.
- On real JetStream: 128 concurrent three-message submissions with stored subject/payload verification, then 32 competing comparisons with exactly one success.
- Real whole-submission rejection on a stale comparison, capacity rejection, and disabled atomic publishing.
- A real committed write with its receipt withheld: uncertain result, exactly one stored write, and no replay.
- A NATS responder fixture with wrong stream/batch/count/sequence, malformed JSON, missing replies, definite rejections, conflicts, and a valid receipt.

## Reproduction

Use a dedicated NATS instance, with JetStream enabled, on `NATS_URL`. Tests and examples create unique stream names and delete only those streams.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all
NATS_URL=nats://127.0.0.1:4222 cargo test --locked --test nats -- --ignored
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
NATS_URL=nats://127.0.0.1:4222 cargo run --locked --example basic
NATS_URL=nats://127.0.0.1:4222 cargo run --locked --example conflict_isolation
NATS_URL=nats://127.0.0.1:4222 cargo run --locked --example admission_policy
cargo package --locked
```

CI executes the checks on Rust 1.88 and stable against NATS 2.12.1 and 2.15.0. Local native testing uses Rust 1.98.1 on macOS with both dedicated server versions.

## Limits of this evidence

Receipt withholding models loss after a confirmed commit. The wire fixture checks transport response handling; it is not a storage engine. Neither establishes behavior under cluster leader changes, network partitions, server crashes, or multi-node storage failure. This release makes no throughput, latency, or application exactly-once claim.
