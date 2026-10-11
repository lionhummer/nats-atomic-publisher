# Verification record

Verified locally using Rust 1.88 in a Linux container against a dedicated NATS 2.12.1 server.

- Formatting check and Clippy with warnings denied: passed.
- Ten transport-independent regression tests: passed.
- Real-server integration test: passed (receipts, stale-comparison isolation, indivisible rejection).
- README code example compile test: passed.
- Rustdoc with warnings denied: passed.
- All three runnable examples: passed against the real server.

Native executable startup initially stalled on the development Mac, so Linux verification was used. Native unit/doc tests and current-toolchain Clippy subsequently passed too. No multi-node failover or performance qualification is claimed. CI repeats formatting, Clippy, unit/doc tests and the real-server test.
