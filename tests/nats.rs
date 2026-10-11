use nats_atomic_publisher::{
    AtomicPublisher, CancellationToken, Config, Error, NatsBackend, Publication,
};
use std::{sync::Arc, time::Duration};
/// Run explicitly with NATS_URL set. Never connects to a broker during unit tests.
#[tokio::test]
#[ignore = "requires a dedicated NATS server; see README"]
async fn real_atomicity_conflicts_and_receipts() {
    let client = async_nats::connect(std::env::var("NATS_URL").expect("set NATS_URL"))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let name = format!("TEST_{}", uuid::Uuid::now_v7().simple());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: name.clone(),
        subjects: vec![format!("{name}.>")],
        allow_atomic_publish: true,
        allow_direct: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(NatsBackend::new(client, Duration::from_secs(3)).unwrap()),
        &name,
        Config {
            batch_window: Duration::from_millis(20),
            max_in_flight: 1,
            ..Default::default()
        },
        shutdown.clone(),
    )
    .unwrap();
    let c = CancellationToken::new();
    let receipt = p
        .publish(
            vec![
                Publication::new(format!("{name}.a"), "a").expected_subject_sequence(0),
                Publication::new(format!("{name}.b"), "b").expected_subject_sequence(0),
            ],
            &c,
        )
        .await
        .unwrap();
    assert_eq!((receipt.first_sequence, receipt.last_sequence), (1, 2));
    let (a, b) = tokio::join!(
        p.publish(
            vec![Publication::new(format!("{name}.a"), "stale").expected_subject_sequence(0)],
            &c
        ),
        p.publish(
            vec![Publication::new(format!("{name}.c"), "fresh").expected_subject_sequence(0)],
            &c
        )
    );
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b.unwrap().last_sequence, 3);
    // A stale member rejects an entire indivisible submission, including a fresh member.
    let rejected = p
        .publish(
            vec![
                Publication::new(format!("{name}.a"), "bad").expected_subject_sequence(0),
                Publication::new(format!("{name}.d"), "must-not-land"),
            ],
            &c,
        )
        .await;
    assert_eq!(rejected, Err(Error::Conflict));
    let stream = js.get_stream(&name).await.unwrap();
    assert_eq!(stream.cached_info().state.messages, 3);
    assert!(
        stream
            .direct_get_last_for_subject(format!("{name}.d"))
            .await
            .is_err()
    );
    shutdown.cancel();
    js.delete_stream(name).await.unwrap();
}
