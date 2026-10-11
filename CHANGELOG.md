# Changelog

## 0.1.0 — 2026-10-11

- Indivisible JetStream submissions with bounded concurrent batching and per-caller receipts.
- Subject-comparison conflict isolation with server revalidation after inspection.
- Independent ordinary/priority budgets and injectable admission/transport policies.
- Explicit non-admission, rejection, and uncertain-outcome errors; no uncertain replay.
- Shared validation for direct and queued publication, ambiguous comparison rejection, and safe timer validation.
- Eighteen regression tests, five dedicated-broker tests, executable examples, and a Rust/NATS compatibility CI matrix.
- Dual MIT OR Apache-2.0 licensing.

Cluster partitions, leader elections, and throughput qualification are outside this release's tested scope.
