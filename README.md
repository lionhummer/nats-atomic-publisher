# NATS Atomic Publisher

**Concurrent Rust publication into one JetStream stream, with automatic atomic batching, conflict isolation, and bounded queues.**

Submit related messages as one indivisible operation. The publisher can combine independent operations into a larger server batch, return a receipt to each caller, and isolate stale subject comparisons after a definite rejection.

NATS supplies atomicity. This library supplies the coordination around it.

> Version 0.2.0. Rust 1.88+, Tokio with its time driver enabled, async-nats 0.50. Tested against NATS 2.12.1 and 2.15.0 with atomic publishing enabled. Match client limits to your server configuration. Distributed under MIT OR Apache-2.0.

## Why this exists

Suppose 100 workflow updates arrive together. Each contains a state change and an event that must land together. Combining their writes saves transactions, but one stale optimistic-concurrency comparison can reject the whole batch.

The publisher inspects subject sequences after that definite rejection, reports stale operations as conflicts, and submits the remaining candidates again. Their comparisons remain attached: the server checks them again at commit. A read during isolation never grants permission to overwrite newer state.

This is useful for workflow journals, task schedulers, state machines, and other applications with independent, conditional multi-message writes into one stream.

## Features

- Indivisible submissions, even when combined with other callers.
- Configurable batching window, message/byte limits, queue sizes, and in-flight concurrency.
- Separate ordinary and priority byte budgets.
- Conflict isolation with bounded, timed subject-sequence reads.
- Per-submission first/final stream sequences.
- Explicit non-publication versus uncertain-outcome errors, including cancellation of queued writes.
- Existing async-nats connections: keep your authentication, TLS, and reconnect configuration.
- An injectable `Backend` for capacity policies, testing, and transport integration.

## Quick start

Install from crates.io:

```toml
[dependencies]
nats-atomic-publisher = "0.2"
async-nats = "0.50"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use nats_atomic_publisher::{
    AtomicPublisher, CancellationToken, Config, NatsBackend, Publication,
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = async_nats::connect("nats://127.0.0.1:4222").await?;
let inspection_client = async_nats::connect("nats://127.0.0.1:4222").await?;
let js = async_nats::jetstream::new(client.clone());
js.create_stream(async_nats::jetstream::stream::Config {
    name: "WORKFLOWS".into(),
    subjects: vec!["workflows.>".into()],
    allow_atomic_publish: true,
    allow_direct: true, // required for conflict inspection
    ..Default::default()
}).await?;

let shutdown = CancellationToken::new();
let publisher = AtomicPublisher::new(
    Arc::new(NatsBackend::with_clients(client, inspection_client, Duration::from_secs(3))?),
    "WORKFLOWS",
    Config::default(),
    shutdown.clone(),
)?;
let receipt = publisher.publish(
    vec![
        Publication::new("workflows.job42.state", "running")
            .expected_subject_sequence(0),
        Publication::new("workflows.job42.events", "started"),
    ],
    &CancellationToken::new(),
).await?;
println!("Stored sequences {}..={}", receipt.first_sequence, receipt.last_sequence);
shutdown.cancel();
Ok(())
}
```

The expected sequence is a **stream sequence on that subject**, not a per-subject message count. Zero means the subject has no previous message.

| Example | Command | Demonstrates |
| --- | --- | --- |
| [Basic](examples/basic.rs) | `cargo run --example basic` | Stream setup and an acknowledged submission |
| [Conflict isolation](examples/conflict_isolation.rs) | `cargo run --example conflict_isolation` | A stale submission alongside an independent write |
| [Admission policy](examples/admission_policy.rs) | `cargo run --example admission_policy` | Ordinary traffic gated while priority completion proceeds |

The runnable [basic example](examples/basic.rs) creates a uniquely named stream and deletes it after the demonstration:

```sh
docker run -d --name atomic-demo -p 127.0.0.1:4222:4222 nats:2.12.1 -js
NATS_URL=nats://127.0.0.1:4222 cargo run --example basic
docker rm -f atomic-demo
```

## Configuration

| Field | Default | Purpose |
| --- | --- | --- |
| `queue_capacity` | 2048 | Queued ordinary submissions |
| `priority_queue_capacity` | 256 | Queued priority submissions |
| `queue_bytes` | 64 MiB | Estimated ordinary bytes queued/in flight |
| `priority_queue_bytes` | 8 MiB | Independent priority byte allowance |
| `max_batch_messages` | 1000 | Maximum messages per combined batch |
| `max_batch_bytes` | 4 MiB | Maximum estimated bytes per batch/submission |
| `max_in_flight` | 2 | Concurrent write tasks per publisher |
| `batch_window` | 1 ms | Maximum collection delay |
| `request_timeout` | 5 s | Caller deadline and backend-operation deadline |
| `inspection_timeout` | 1 s | Whole conflict-inspection deadline |
| `inspection_concurrency` | 16 | Concurrent submission inspections within each group |
| `max_inspections` | 2 | Concurrent conflict-inspection groups, independent of writes |

Byte accounting includes payload, subject, headers, and a conservative per-message allowance. It bounds retained publication estimates; it does not measure allocator overhead or cap callers' own allocations before admission. Configure all publisher instances together against stream/server limits.

Overlapping subject writes are not combined. Global stream-sequence comparisons and alternate-subject comparisons are kept in separate batches. A global sequence comparison may appear only on the first message. A later write to an already-written subject cannot carry another subject-sequence comparison. Mismatched expected streams, duplicate comparison headers, and reserved batch headers are rejected before admission. Batches may commit out of submission order when `max_in_flight > 1`; use subject comparisons to enforce application ordering. No ordering guarantee is made across processes.

## Priority and capacity

`publish_priority` uses a separate local queue and byte allowance. Compatible priority submissions share batches under the same collection window and limits as ordinary submissions. It is scheduled ahead of ordinary work when a slot opens, and bypasses `Backend::admit`. Use it for small settlement or recovery records that should remain admissible when ordinary traffic saturates the local queue.

This does **not** reserve server disk, preempt a running batch, or guarantee fairness. Sustained priority traffic can starve ordinary work. A custom backend may enforce an ordinary-write high-water policy while leaving server capacity for completion records. Capacity observations are advisory; the server remains responsible for storage limits.

See [adaptation examples](docs/adapting.md) for policy hooks and integration boundaries.

## Error and retry contract

| Error | Meaning | Caller action |
| --- | --- | --- |
| `Invalid` | Invalid input/configuration; never admitted | Fix the input |
| `Closed` | Publisher closed before admission | Use a live publisher |
| `NotAdmitted` | Cancelled/timed out before queue admission | Safe to resubmit |
| `NotPublished` | Queued work abandoned before transmission | Safe to resubmit |
| `Conflict` | Definite server comparison rejection | Read current state; form a new operation |
| `Rejected` | Definite server rejection | Resolve capacity/configuration/unsupported operation |
| `Unavailable` | Setup failed before commit, admission policy failed, or inspection failed | Diagnose and retry according to application policy |
| `Indeterminate` | Commit may have happened | Reconcile before retrying |
| `InvalidReceipt` | Receipt cannot prove the expected commit | Reconcile before retrying |

Only `Conflict` triggers automatic isolation, and only when an actual stale subject comparison is found. Other rejections and uncertain results never trigger automatic replay. Subject-read failures remain `Unavailable`, even if isolation cannot identify a stale comparison.

**Cancellation and deadlines are checked at the transmission boundary.** Before queue admission, cancellation returns `NotAdmitted`. After admission but before transport starts, it returns `NotPublished` and the submission will not be sent. Once transport starts, cancellation returns `Indeterminate`; it cannot retract a publication. Dropping a pending future also abandons work that has not started transmission. A definite conflict allows the remaining candidates to return to the cancellable state before retry. Include stable operation identities in your application records, then read them back to decide whether the intended operation committed. This library does not implement application reconciliation or make an exactly-once claim.

Dropping all publisher handles drains live admitted work. Cancelling the shared shutdown token aborts the pump; queued writes return `NotPublished`, while writes that started transmission remain uncertain. This version has no explicit graceful-shutdown join API: applications needing a drain barrier should await their publication futures before shutdown.

## Deadlines and independent connections

`publish_until(messages, deadline, cancellation)` and `publish_priority_until` accept an absolute `tokio::time::Instant`, including time waiting for queue and byte capacity. The usual methods use `Config::request_timeout` as the caller deadline. Each backend write attempt is also bounded by `request_timeout`, even if the caller supplies a longer deadline. An expired deadline never permits a new transmission; an in-flight write can still commit after its caller stops waiting.

Use `NatsBackend::with_clients(publication_client, inspection_client, timeout)` with **two independently established connections** for busy workloads. Direct reads sharing a publication connection can delay batch traffic during server-side contention. Both connections must target the same account and stream and have the necessary permissions. Cloning a client shares its socket. `NatsBackend::new` remains available for single-connection use.

Conflict inspection has its own task limit. At most `max_inspections * inspection_concurrency` subject reads are active per publisher. Groups awaiting inspection or retry retain their lane's byte permits, so separating inspection from writes does not create an unbounded memory queue. Subject reads remain timed and never authorize replay of an uncertain write.

### Upgrading from 0.1

Add `NotPublished` to exhaustive error matches and `max_inspections` to exhaustive `Config` literals (or use `..Config::default()`). Callers must not depend on abandoned queued work eventually publishing. Priority operations can now share an atomic batch: a definite non-comparison rejection can affect all members, as it already could on the ordinary lane. No automatic replay follows uncertain outcomes. Alternate-subject and global comparisons retain the conservative exclusive-batch behavior of 0.1.

## Boundaries

The server supplies the atomic batch protocol. This package coordinates concurrent submissions, coalescing, per-caller receipts, local priority budgets, and subject-comparison conflict isolation. It does not provide cross-stream transactions, consumer-side atomic delivery, or a workflow engine.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

Unit tests use injected backends and require no broker. The real-server test is explicitly ignored in the default suite:

```sh
NATS_URL=nats://127.0.0.1:4222 cargo test --test nats -- --ignored
```

Use a dedicated test server. The test creates and deletes only its uniquely named stream. The six broker tests check real concurrent receipt ranges, comparison contention, whole-submission rejection, capacity/feature rejection, committed writes with withheld receipts, malformed wire acknowledgments, priority batching, and cancellation before transmission. CI runs both suites on the minimum supported and stable Rust toolchains against NATS 2.12.1 and 2.15.0. No throughput claim is made without a recorded benchmark.

The protocol follows [NATS ADR-50](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md). `Nats-Expected-Last-Msg-Id` is unsupported by atomic batches; the server rejects it. `Nats-Msg-Id` deduplication is supported from NATS 2.12.1, but a duplicate rejects the entire combined batch.

Read [the design](docs/design.md), [contributor instructions](AGENTS.md), and [release preparation](RELEASE.md).
