# Design and correctness boundaries

## Ownership

The host owns its async-nats connection, stream provisioning, authentication, retention policy, application identities and reconciliation. `NatsBackend` owns wire staging and receipt validation. `AtomicPublisher` owns queue admission, local byte permits, batch collection, conflict isolation, and receipt distribution.

A submission is never split. The ordinary collector combines submissions only while byte/message limits allow and subjects do not overlap. Global state comparisons are exclusive. Collection introduces at most the configured batch window; queue wait and server work may take longer.

## Admission

Acquire a lane's byte permit and reserve a channel slot before sending. A deadline or cancellation before sending returns `NotAdmitted`. Sending is synchronous after reservation. Thereafter timeout/cancellation is `Indeterminate`. Pending records retain their byte permit until completion or pump shutdown, even if a caller stops awaiting.

Priority uses separate permits; the same in-flight slots service both lanes. It bypasses only the injectable admission policy, never server enforcement.

## Receipt handling

The backend stages each batch with a UUIDv7 identity, checks the setup response, and commits with a final request. The final receipt must match stream, batch ID, count and a valid final sequence. The pump subtracts total messages and partitions contiguous stream sequences among callers.

Stream atomicity does not imply atomic consumption: consumers can observe and acknowledge messages separately. No assumption of cross-stream atomicity is made.

## Conflict isolation

After a definite comparison rejection, inspect each submission's expected subject sequences under bounded concurrency and a whole-pass timeout. An identified stale submission gets `Conflict`; remaining submissions are rechecked by the server on resubmission. Read failures are reported, not converted into permission to write. If no actual stale comparison is identified, fail the group rather than loop. Every retry removes at least one conflicting submission, so isolation retries are bounded by initial group size.

## Adaptation and tests

The `Backend` contract allows deterministic tests without a broker. An implementation must never return a definite error when commit may have happened. Capacity hooks must follow the same contract. Test backends cover lost receipts, invalid acknowledgments, overlap, priority and cancellation. The dedicated NATS test checks actual server behavior. Fault injection into network leader changes and multi-node performance qualification remain release-hardening work; unit tests do not establish those properties.
