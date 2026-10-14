# Adapting the publisher

## Reuse your connection

Create `async_nats::Client` using your application's existing `ConnectOptions` (TLS, credentials, connection timeout and reconnect policy), then pass it to `NatsBackend::new`, or establish two connections with those options and pass them to `NatsBackend::with_clients` to separate writes from inspection reads. Both must access the same account and stream; cloning one client does not create another socket. The library does not load credentials or environment files.

## Add an admission policy

Wrap `NatsBackend` in a type implementing `Backend`. The admission hook must not publish or perform submission side effects. Implement `admit(messages)` to check your account's storage allowance, application quota, or maintenance gate. Delegate `atomic` and `last_subject_sequence` to the inner backend. Use `async_trait` for the implementation and wrap network work in explicit deadlines. Return `Rejected` only for definite admission rejection. Priority submissions bypass `admit` so this hook is not an authorization boundary.

The publisher bounds each backend write with `request_timeout`, but configure the transport's own request timeouts too. Reading capacity before a write is advisory: concurrent processes can consume capacity after the read.

## Application identity and reconciliation

Put an operation ID in your durable records. On an uncertain outcome, read the intended records and compare that ID, payload hash, and expected state transition. If they prove commit, recover the success. If the outcome remains unknown, do not blindly replay a provider call or other external effect. Deduplication windows alone do not solve indefinite reconciliation.

## Choosing limits

For low latency, reduce `batch_window` or set it to zero. For larger operations, raise `max_batch_bytes` and the lane's byte allowance together. For ordering-sensitive journals, start with `max_in_flight = 1`. Across many processes, divide the server's in-flight allowance among publishers with headroom for other clients.

Use `publish_priority` only for bounded completion/recovery work. It has no fairness quota; high sustained priority volume should use a separate application policy or publisher.
