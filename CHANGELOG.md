# Changelog

## 0.2.0 — 2026-10-14

- Cancel queued submissions safely before transmission, including during admission checks and after definite conflict rejection. Dropped futures abandon unsent work.
- Add `Error::NotPublished` for definite non-publication after queue admission; retain uncertain outcomes once transmission starts.
- Batch compatible priority submissions under the same limits as ordinary submissions.
- Schedule bounded conflict inspection separately from publication slots, preserving lane priority on retry and byte-budget bounds throughout.
- Add `Config::max_inspections`, plus absolute-deadline `publish_until` and `publish_priority_until` methods.
- Add `NatsBackend::with_clients` for independently connected publication and inspection clients; preserve the single-connection constructor.
- Expand coverage to 28 backend regression tests, six dedicated-broker tests and the compiled README example.

Migration: update exhaustive `Error` matches and `Config` literals. Queued cancellation no longer implies eventual publication. Priority submissions can now share a server rejection. The admission hook must remain free of submission side effects. See README for the complete contract.

## 0.1.0 — 2026-10-11

- Indivisible JetStream submissions with bounded concurrent batching and per-caller receipts.
- Subject-comparison conflict isolation with server revalidation after inspection.
- Independent ordinary/priority budgets and injectable admission/transport policies.
- Explicit non-admission, rejection, and uncertain-outcome errors; no uncertain replay.
- Shared validation for direct and queued publication, ambiguous comparison rejection, and safe timer validation.
- Eighteen regression tests, five dedicated-broker tests, executable examples, and a Rust/NATS compatibility CI matrix.
- Dual MIT OR Apache-2.0 licensing.

Cluster partitions, leader elections, and throughput qualification are outside this release's tested scope.
