use nats_atomic_publisher::{
    AtomicPublisher, Backend, CancellationToken, Config, Error, NatsBackend, Publication,
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
    let inspection_client = async_nats::connect(std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(
            NatsBackend::with_clients(client, inspection_client, Duration::from_secs(3)).unwrap(),
        ),
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

async fn setup(
    atomic: bool,
    max_messages: i64,
) -> (async_nats::Client, async_nats::jetstream::Context, String) {
    let client = async_nats::connect(std::env::var("NATS_URL").expect("set NATS_URL"))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let name = format!("TEST_{}", uuid::Uuid::now_v7().simple());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: name.clone(),
        subjects: vec![format!("{name}.>")],
        allow_atomic_publish: atomic,
        allow_direct: true,
        max_messages,
        discard: async_nats::jetstream::stream::DiscardPolicy::New,
        ..Default::default()
    })
    .await
    .unwrap();
    (client, js, name)
}

#[tokio::test]
#[ignore = "requires a dedicated NATS server"]
async fn concurrent_real_receipts_and_compare_and_set_contention() {
    let (client, js, name) = setup(true, -1).await;
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(NatsBackend::new(client, Duration::from_secs(5)).unwrap()),
        &name,
        Config {
            max_in_flight: 4,
            max_batch_messages: 64,
            request_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        shutdown.clone(),
    )
    .unwrap();
    let results = futures::future::join_all((0..128).map(|i| {
        let p = p.clone();
        let name = name.clone();
        async move {
            p.publish(
                (0..3)
                    .map(|j| {
                        Publication::new(format!("{name}.{i}.{j}"), format!("{i}:{j}"))
                            .expected_subject_sequence(0)
                    })
                    .collect(),
                &CancellationToken::new(),
            )
            .await
            .unwrap()
        }
    }))
    .await;
    let stream = js.get_stream(&name).await.unwrap();
    let mut seqs = vec![];
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r.messages, 3);
        for (j, seq) in (r.first_sequence..=r.last_sequence).enumerate() {
            let m = stream.direct_get(seq).await.unwrap();
            assert_eq!(m.subject.as_str(), format!("{name}.{i}.{j}"));
            assert_eq!(m.payload.as_ref(), format!("{i}:{j}").as_bytes());
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=384).collect::<Vec<_>>());
    let results = futures::future::join_all((0..32).map(|i| {
        let p = p.clone();
        let name = name.clone();
        async move {
            p.publish(
                vec![
                    Publication::new(format!("{name}.contended"), i.to_string())
                        .expected_subject_sequence(0),
                ],
                &CancellationToken::new(),
            )
            .await
        }
    }))
    .await;
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(Error::Conflict))
            .count(),
        31
    );
    assert_eq!(
        js.get_stream(&name)
            .await
            .unwrap()
            .cached_info()
            .state
            .messages,
        385
    );
    shutdown.cancel();
    js.delete_stream(name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a dedicated NATS server"]
async fn server_capacity_and_disabled_atomicity_reject_without_partial_storage() {
    for enabled in [true, false] {
        let (client, js, name) = setup(enabled, 1).await;
        let backend = NatsBackend::new(client, Duration::from_secs(2)).unwrap();
        assert_eq!(
            backend
                .atomic(
                    &name,
                    vec![
                        Publication::new(format!("{name}.a"), "a"),
                        Publication::new(format!("{name}.b"), "b")
                    ]
                )
                .await,
            Err(Error::Rejected)
        );
        assert_eq!(
            js.get_stream(&name)
                .await
                .unwrap()
                .cached_info()
                .state
                .messages,
            0
        );
        js.delete_stream(name).await.unwrap();
    }
}

struct LostReceipt {
    inner: NatsBackend,
    committed: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl nats_atomic_publisher::Backend for LostReceipt {
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.atomic(s, m).await?;
        self.committed.notify_one();
        futures::future::pending().await
    }
    async fn last_subject_sequence(&self, s: &str, subject: &str) -> Result<u64, Error> {
        self.inner.last_subject_sequence(s, subject).await
    }
}
#[tokio::test]
#[ignore = "requires a dedicated NATS server"]
async fn committed_write_with_lost_receipt_is_uncertain_and_never_replayed() {
    let (client, js, name) = setup(true, -1).await;
    let backend = Arc::new(LostReceipt {
        inner: NatsBackend::new(client, Duration::from_secs(2)).unwrap(),
        committed: tokio::sync::Notify::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let p = AtomicPublisher::new(
        backend.clone(),
        &name,
        Config {
            request_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let task = tokio::spawn({
        let name = name.clone();
        async move {
            p.publish(
                vec![Publication::new(
                    format!("{name}.a"),
                    "reconcile-this-operation",
                )],
                &CancellationToken::new(),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(3), backend.committed.notified())
        .await
        .unwrap();
    assert_eq!(task.await.unwrap(), Err(Error::Indeterminate));
    assert_eq!(
        js.get_stream(&name)
            .await
            .unwrap()
            .cached_info()
            .state
            .messages,
        1
    );
    assert_eq!(backend.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    js.delete_stream(name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a dedicated NATS server"]
async fn wire_receipt_validation_and_missing_acknowledgments() {
    use futures::StreamExt;
    let client = async_nats::connect(std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    let backend = NatsBackend::new(client.clone(), Duration::from_millis(150)).unwrap();
    for mode in [
        "stream", "batch", "count", "sequence", "json", "missing", "rejected", "conflict", "valid",
    ] {
        let subject = format!("WIRE.{}", uuid::Uuid::now_v7().simple());
        let mut sub = client.subscribe(subject.clone()).await.unwrap();
        client.flush().await.unwrap();
        let service = tokio::spawn({
            let client = client.clone();
            async move {
                let m = sub.next().await.unwrap();
                let batch = m
                    .headers
                    .as_ref()
                    .unwrap()
                    .get("Nats-Batch-Id")
                    .unwrap()
                    .as_str();
                let mut ack = serde_json::json!({"stream":"TEST","seq":1,"batch":batch,"count":1});
                match mode {
                    "stream" => ack["stream"] = "OTHER".into(),
                    "batch" => ack["batch"] = "OTHER".into(),
                    "count" => ack["count"] = 2.into(),
                    "sequence" => ack["seq"] = 0.into(),
                    "rejected" => ack = serde_json::json!({"error":{"err_code":10174}}),
                    "conflict" => ack = serde_json::json!({"error":{"err_code":10071}}),
                    _ => {}
                }
                if mode != "missing" {
                    client
                        .publish(
                            m.reply.unwrap(),
                            if mode == "json" {
                                "broken".into()
                            } else {
                                ack.to_string().into()
                            },
                        )
                        .await
                        .unwrap();
                    client.flush().await.unwrap();
                }
                assert!(
                    tokio::time::timeout(Duration::from_millis(200), sub.next())
                        .await
                        .is_err(),
                    "transport must not replay"
                );
            }
        });
        let result = backend
            .atomic("TEST", vec![Publication::new(subject, "a")])
            .await;
        assert_eq!(
            result,
            match mode {
                "valid" => Ok(1),
                "missing" => Err(Error::Indeterminate),
                "rejected" => Err(Error::Rejected),
                "conflict" => Err(Error::Conflict),
                _ => Err(Error::InvalidReceipt),
            }
        );
        service.await.unwrap();
    }
    let mut bad = Publication::new("invalid", "x");
    bad.headers.insert("Nats-Batch-Commit", "1");
    assert_eq!(backend.atomic("TEST", vec![bad]).await, Err(Error::Invalid));
}

#[tokio::test]
#[ignore = "requires a dedicated NATS server"]
async fn priority_batches_and_cancelled_policy_waits_on_real_server() {
    use async_trait::async_trait;
    use std::sync::Mutex;
    struct Gated {
        inner: NatsBackend,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        batches: Mutex<Vec<usize>>,
    }
    #[async_trait]
    impl Backend for Gated {
        async fn admit(&self, _: &[Publication]) -> Result<(), Error> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
        async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
            self.batches.lock().unwrap().push(m.len());
            self.inner.atomic(s, m).await
        }
        async fn last_subject_sequence(&self, s: &str, t: &str) -> Result<u64, Error> {
            self.inner.last_subject_sequence(s, t).await
        }
    }
    let (client, js, name) = setup(true, -1).await;
    let reads = async_nats::connect(std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    let backend = Arc::new(Gated {
        inner: NatsBackend::with_clients(client, reads, Duration::from_secs(3)).unwrap(),
        entered: Default::default(),
        release: Default::default(),
        batches: Default::default(),
    });
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        backend.clone(),
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
    let (a, b) = tokio::join!(
        p.publish_priority(vec![Publication::new(format!("{name}.a"), "a")], &c),
        p.publish_priority(vec![Publication::new(format!("{name}.b"), "b")], &c),
    );
    assert_eq!(a.unwrap().first_sequence, 1);
    assert_eq!(b.unwrap().last_sequence, 2);
    assert_eq!(*backend.batches.lock().unwrap(), [2]);
    let task = tokio::spawn({
        let p = p.clone();
        let name = name.clone();
        let c = c.clone();
        async move {
            p.publish(
                vec![Publication::new(
                    format!("{name}.cancelled"),
                    "must-not-land",
                )],
                &c,
            )
            .await
        }
    });
    backend.entered.notified().await;
    c.cancel();
    assert_eq!(task.await.unwrap(), Err(Error::NotPublished));
    backend.release.notify_one();
    assert_eq!(
        p.publish_priority(
            vec![Publication::new(format!("{name}.barrier"), "done")],
            &CancellationToken::new()
        )
        .await
        .unwrap()
        .last_sequence,
        3
    );
    assert_eq!(
        backend
            .inner
            .last_subject_sequence(&name, &format!("{name}.cancelled"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        js.get_stream(&name)
            .await
            .unwrap()
            .cached_info()
            .state
            .messages,
        3
    );
    shutdown.cancel();
    js.delete_stream(name).await.unwrap();
}
