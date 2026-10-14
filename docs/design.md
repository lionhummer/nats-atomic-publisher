# Design and correctness boundaries

## Ownership

The host owns its async-nats connection, stream provisioning, authentication, retention policy, application identities and reconciliation. `NatsBackend` owns wire staging and receipt validation. `AtomicPublisher` owns queue admission, local byte permits, batch collection, conflict isolation, and receipt distribution.

A submission is never split. Both lane collectors combine submissions only while byte/message limits allow and subjects do not overlap. Global state comparisons are exclusive. Collection introduces at most the configured batch window; queue wait and server work may take longer.

## Admission

Acquire a lane's byte permit and reserve a channel slot before sending. A deadline or cancellation before sending returns `NotAdmitted`. Sending is synchronous after reservation. Each pending record has a deadline and an atomic queued/publishing/abandoned phase. Cancellation or dropping a future can abandon queued work; explicit cancellation then returns `NotPublished`. Immediately before calling the transport, after the admission policy completes, the publisher atomically claims each live submission. Cancellation after that claim is `Indeterminate`. This boundary is conservative: transport may not yet have sent bytes. Pending records retain their byte permit until discarded, completed or the pump shuts down. A custom `Backend::admit` must be a side-effect-free capacity check: it must never publish the submission.

Priority uses separate permits; the same in-flight slots service both lanes. It bypasses only the injectable admission policy, never server enforcement.

## Receipt handling

The backend stages each batch with a UUIDv7 identity, checks the setup response, and commits with a final request. The final receipt must match stream, batch ID, count and a valid final sequence. The pump subtracts total messages and partitions contiguous stream sequences among callers.

Stream atomicity does not imply atomic consumption: consumers can observe and acknowledge messages separately. No assumption of cross-stream atomicity is made.

## Conflict isolation

Write futures release their publication slots after a definite comparison rejection. Separately bounded inspection futures inspect each submission's expected subject sequences under bounded concurrency and a whole-pass timeout. An identified stale submission gets `Conflict`; remaining submissions are rechecked by the server on resubmission. Read failures are reported, not converted into permission to write. If no actual stale comparison is identified, fail the group rather than loop. Every retry removes at least one conflicting submission, so isolation retries are bounded by initial group size. Only a definite rejection resets the publishing phase to queued. Retry groups preserve their lane and byte permits. Cancellation and deadlines are rechecked before retry transmission. Inspection and retry queues remain bounded by the retained lane byte budgets.

## Adaptation and tests

The `Backend` contract allows deterministic tests without a broker. An implementation must never return a definite error when commit may have happened. Capacity hooks must follow the same contract. Test backends cover lost receipts, invalid acknowledgments, overlap, priority, cancellation, inspection failures, and concurrent changes between inspection and retry. Both queued and direct transports validate reserved headers and comparison shape before IO. Unrepresentable timer durations return `Invalid` rather than panicking. The dedicated NATS test checks actual server behavior. A committed-write receipt-loss test and a NATS responder fixture exercise uncertain outcomes and wire receipt validation. Network partition/leader-election testing and multi-node performance qualification are outside the 0.2.0 verification scope; these tests do not establish those properties.
