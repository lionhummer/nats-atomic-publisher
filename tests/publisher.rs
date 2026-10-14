use async_trait::async_trait;
use nats_atomic_publisher::{
    AtomicPublisher, Backend, CancellationToken, Config, Error, Publication,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Fake {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    seq: u64,
    subjects: HashMap<String, u64>,
    calls: Vec<Vec<String>>,
    error: Option<Error>,
    unknown_conflict: bool,
    invalid_receipt: bool,
}
#[async_trait]
impl Backend for Fake {
    async fn atomic(&self, _: &str, messages: Vec<Publication>) -> Result<u64, Error> {
        let mut s = self.state.lock().unwrap();
        s.calls
            .push(messages.iter().map(|m| m.subject.clone()).collect());
        if let Some(error) = s.error {
            return Err(error);
        }
        if s.unknown_conflict {
            return Err(Error::Conflict);
        }
        if s.invalid_receipt {
            return Ok(0);
        }
        for m in &messages {
            if m.headers
                .get("Nats-Expected-Last-Subject-Sequence")
                .is_some_and(|e| {
                    e.as_str().parse::<u64>().unwrap() != *s.subjects.get(&m.subject).unwrap_or(&0)
                })
            {
                return Err(Error::Conflict);
            }
        }
        for m in messages {
            s.seq += 1;
            let seq = s.seq;
            s.subjects.insert(m.subject, seq);
        }
        Ok(s.seq)
    }
    async fn last_subject_sequence(&self, _: &str, subject: &str) -> Result<u64, Error> {
        Ok(*self
            .state
            .lock()
            .unwrap()
            .subjects
            .get(subject)
            .unwrap_or(&0))
    }
}
fn publisher(fake: Arc<Fake>, config: Config) -> AtomicPublisher {
    AtomicPublisher::new(fake, "TEST", config, CancellationToken::new()).unwrap()
}
fn config() -> Config {
    Config {
        batch_window: Duration::from_millis(20),
        max_in_flight: 1,
        ..Default::default()
    }
}
#[tokio::test]
async fn coalesces_and_returns_each_submission_sequence() {
    let fake = Arc::new(Fake::default());
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(
            vec![Publication::new("a", "1"), Publication::new("b", "2")],
            &c
        ),
        p.publish(vec![Publication::new("c", "3")], &c)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!((a.first_sequence, a.last_sequence, a.messages), (1, 2, 2));
    assert_eq!(b.last_sequence, 3);
    assert_eq!(fake.state.lock().unwrap().calls.len(), 1);
}
#[tokio::test]
async fn isolates_stale_groups_without_dropping_fresh_writes() {
    let fake = Arc::new(Fake::default());
    fake.state
        .lock()
        .unwrap()
        .subjects
        .insert("stale".into(), 1);
    fake.state.lock().unwrap().seq = 1;
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(
            vec![Publication::new("stale", "1").expected_subject_sequence(0)],
            &c
        ),
        p.publish(
            vec![Publication::new("fresh", "2").expected_subject_sequence(0)],
            &c
        )
    );
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b.unwrap().last_sequence, 2);
    assert_eq!(fake.state.lock().unwrap().calls.len(), 2);
}
#[tokio::test]
async fn uncertain_receipts_are_never_retried() {
    let fake = Arc::new(Fake::default());
    fake.state.lock().unwrap().error = Some(Error::Indeterminate);
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(vec![Publication::new("a", "1")], &c),
        p.publish(vec![Publication::new("b", "2")], &c)
    );
    assert_eq!(a, Err(Error::Indeterminate));
    assert_eq!(b, Err(Error::Indeterminate));
    assert_eq!(fake.state.lock().unwrap().calls.len(), 1);
}
#[tokio::test]
async fn unrecognized_conflict_does_not_loop() {
    let fake = Arc::new(Fake::default());
    fake.state.lock().unwrap().unknown_conflict = true;
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(vec![Publication::new("a", "1")], &c),
        p.publish(vec![Publication::new("b", "2")], &c)
    );
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b, Err(Error::Conflict));
    assert_eq!(fake.state.lock().unwrap().calls.len(), 1);
}
#[tokio::test]
async fn overlapping_subjects_stay_in_separate_batches() {
    let fake = Arc::new(Fake::default());
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(vec![Publication::new("a", "1")], &c),
        p.publish(
            vec![Publication::new("a", "2").expected_subject_sequence(1)],
            &c
        )
    );
    assert!(a.is_ok());
    assert!(b.is_ok());
    assert_eq!(fake.state.lock().unwrap().calls.len(), 2);
}
#[tokio::test]
async fn cancellation_before_admission_has_no_effect() {
    let fake = Arc::new(Fake::default());
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    c.cancel();
    assert_eq!(
        p.publish(vec![Publication::new("a", "1")], &c).await,
        Err(Error::NotAdmitted)
    );
    assert!(fake.state.lock().unwrap().calls.is_empty());
}
#[tokio::test]
async fn validates_limits_and_reserved_headers() {
    let fake = Arc::new(Fake::default());
    let p = publisher(
        fake.clone(),
        Config {
            max_batch_bytes: 512,
            ..config()
        },
    );
    let c = CancellationToken::new();
    assert_eq!(
        p.publish(vec![Publication::new("a", "1")], &c).await,
        Err(Error::Invalid)
    );
    let p = publisher(fake, config());
    let mut m = Publication::new("a", "1");
    m.headers.insert("Nats-Batch-Id", "bad");
    assert_eq!(p.publish(vec![m], &c).await, Err(Error::Invalid));
}
#[tokio::test]
async fn invalid_receipt_is_uncertain() {
    let fake = Arc::new(Fake::default());
    fake.state.lock().unwrap().invalid_receipt = true;
    let p = publisher(fake, config());
    let c = CancellationToken::new();
    assert_eq!(
        p.publish(vec![Publication::new("a", "1")], &c).await,
        Err(Error::InvalidReceipt)
    );
    assert!(Error::InvalidReceipt.is_uncertain());
}
struct Slow;
#[async_trait]
impl Backend for Slow {
    async fn atomic(&self, _: &str, _: Vec<Publication>) -> Result<u64, Error> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(1)
    }
    async fn last_subject_sequence(&self, _: &str, _: &str) -> Result<u64, Error> {
        Ok(0)
    }
}
#[tokio::test]
async fn timeout_after_transmission_starts_is_uncertain() {
    let p = AtomicPublisher::new(
        Arc::new(Slow),
        "TEST",
        Config {
            request_timeout: Duration::from_millis(10),
            batch_window: Duration::ZERO,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let c = CancellationToken::new();
    assert_eq!(
        p.publish(vec![Publication::new("a", "1")], &c).await,
        Err(Error::Indeterminate)
    );
}
struct Gate {
    calls: Mutex<Vec<String>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl Backend for Gate {
    async fn atomic(&self, _: &str, m: Vec<Publication>) -> Result<u64, Error> {
        let index = {
            let mut c = self.calls.lock().unwrap();
            c.push(m[0].subject.clone());
            c.len()
        };
        if index == 1 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(index as u64)
    }
    async fn last_subject_sequence(&self, _: &str, _: &str) -> Result<u64, Error> {
        Ok(0)
    }
}
#[tokio::test]
async fn priority_has_separate_budget_when_ordinary_is_saturated() {
    let g = Arc::new(Gate {
        calls: Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            queue_bytes: 520,
            priority_queue_bytes: 520,
            batch_window: Duration::ZERO,
            max_in_flight: 1,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let c = CancellationToken::new();
    let a = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move { p.publish(vec![Publication::new("a", "1")], &c).await }
    });
    g.entered.notified().await;
    let b = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move { p.publish(vec![Publication::new("b", "1")], &c).await }
    });
    let done = p.publish_priority(vec![Publication::new("done", "1")], &c);
    tokio::pin!(done);
    tokio::select! {biased; result=&mut done=>panic!("premature completion: {result:?}"), _=tokio::task::yield_now()=>{}}
    g.release.notify_one();
    a.await.unwrap().unwrap();
    done.await.unwrap();
    b.await.unwrap().unwrap();
    assert_eq!(*g.calls.lock().unwrap(), ["a", "done", "b"]);
}

#[tokio::test]
async fn rejects_extreme_durations_and_ambiguous_inputs_without_io() {
    let fake = Arc::new(Fake::default());
    for cfg in [
        Config {
            max_inspections: 0,
            ..config()
        },
        Config {
            request_timeout: Duration::MAX,
            ..config()
        },
        Config {
            batch_window: Duration::MAX,
            ..config()
        },
        Config {
            inspection_timeout: Duration::MAX,
            ..config()
        },
    ] {
        assert!(matches!(
            AtomicPublisher::new(fake.clone(), "TEST", cfg, CancellationToken::new()),
            Err(Error::Invalid)
        ));
    }
    let p = publisher(fake.clone(), config());
    let mut wrong_stream = Publication::new("a", "1");
    wrong_stream.headers.insert("Nats-Expected-Stream", "OTHER");
    let mut ambiguous = Publication::new("a", "1").expected_subject_sequence(0);
    ambiguous
        .headers
        .append("Nats-Expected-Last-Subject-Sequence", "1");
    let mut late_global = Publication::new("b", "1");
    late_global
        .headers
        .insert("Nats-Expected-Last-Sequence", "0");
    for messages in [
        vec![],
        vec![Publication::new("a..b", "1")],
        vec![Publication::new("a\0b", "1")],
        vec![wrong_stream],
        vec![ambiguous],
        vec![Publication::new("a", "1"), late_global],
        vec![
            Publication::new("a", "1"),
            Publication::new("a", "2").expected_subject_sequence(0),
        ],
    ] {
        assert_eq!(
            p.publish(messages, &CancellationToken::new()).await,
            Err(Error::Invalid)
        );
    }
    assert!(fake.state.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn global_comparisons_and_alternate_subjects_are_not_coalesced() {
    for header in [
        "Nats-Expected-Last-Sequence",
        "Nats-Expected-Last-Subject-Sequence-Subject",
    ] {
        let fake = Arc::new(Fake::default());
        let p = publisher(fake.clone(), config());
        let mut m = Publication::new("a", "1");
        m.headers.insert(
            header,
            if header.ends_with("Subject") {
                "other"
            } else {
                "0"
            },
        );
        let c = CancellationToken::new();
        let (a, b) = tokio::join!(
            p.publish(vec![m], &c),
            p.publish(vec![Publication::new("b", "2")], &c)
        );
        assert!(a.is_ok() && b.is_ok());
        assert_eq!(fake.state.lock().unwrap().calls.len(), 2);
    }
}

#[tokio::test]
async fn concurrent_receipts_cover_every_message_once_with_bounded_batches() {
    let fake = Arc::new(Fake::default());
    let p = publisher(
        fake.clone(),
        Config {
            max_batch_messages: 7,
            max_in_flight: 4,
            ..config()
        },
    );
    let tasks = (0..100).map(|i| {
        let p = p.clone();
        async move {
            p.publish(
                vec![
                    Publication::new(format!("{i}.a"), "a"),
                    Publication::new(format!("{i}.b"), "b"),
                ],
                &CancellationToken::new(),
            )
            .await
            .unwrap()
        }
    });
    let mut sequences = futures::future::join_all(tasks)
        .await
        .into_iter()
        .flat_map(|r| {
            assert_eq!(r.messages, 2);
            assert_eq!(r.last_sequence - r.first_sequence, 1);
            r.first_sequence..=r.last_sequence
        })
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=200).collect::<Vec<_>>());
    assert!(
        fake.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|c| c.len() <= 7)
    );
}

#[tokio::test]
async fn waiting_for_byte_budget_can_cancel_without_admission_and_budget_recovers() {
    let g = Arc::new(Gate {
        calls: Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            queue_bytes: 514,
            batch_window: Duration::ZERO,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let first = tokio::spawn({
        let p = p.clone();
        async move {
            p.publish(vec![Publication::new("a", "1")], &CancellationToken::new())
                .await
        }
    });
    g.entered.notified().await;
    let c = CancellationToken::new();
    let waiting = p.publish(vec![Publication::new("b", "1")], &c);
    tokio::pin!(waiting);
    tokio::select! {biased; r=&mut waiting => panic!("unexpected admission: {r:?}"), _=tokio::task::yield_now()=>{}}
    c.cancel();
    assert_eq!(waiting.await, Err(Error::NotAdmitted));
    g.release.notify_one();
    assert!(first.await.unwrap().is_ok());
    assert!(
        p.publish(vec![Publication::new("c", "1")], &CancellationToken::new())
            .await
            .is_ok()
    );
    assert_eq!(*g.calls.lock().unwrap(), ["a", "c"]);
}

#[tokio::test]
async fn cancellation_after_transport_starts_is_uncertain_and_not_replayed() {
    let g = Arc::new(Gate {
        calls: Mutex::new(vec![]),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(g.clone(), "TEST", config(), shutdown.clone()).unwrap();
    let c = CancellationToken::new();
    let task = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move { p.publish(vec![Publication::new("a", "1")], &c).await }
    });
    g.entered.notified().await;
    c.cancel();
    assert_eq!(task.await.unwrap(), Err(Error::Indeterminate));
    g.release.notify_one();
    assert!(
        p.publish(vec![Publication::new("b", "1")], &CancellationToken::new())
            .await
            .is_ok()
    );
    shutdown.cancel();
    assert_eq!(
        p.publish(vec![Publication::new("c", "1")], &CancellationToken::new())
            .await,
        Err(Error::Closed)
    );
    assert_eq!(*g.calls.lock().unwrap(), ["a", "b"]);
}

struct InspectionRace {
    fake: Fake,
}
#[async_trait]
impl Backend for InspectionRace {
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.fake.atomic(s, m).await
    }
    async fn last_subject_sequence(&self, s: &str, subject: &str) -> Result<u64, Error> {
        let observed = self.fake.last_subject_sequence(s, subject).await?;
        if subject == "fresh" {
            self.fake
                .state
                .lock()
                .unwrap()
                .subjects
                .insert(subject.into(), 99);
        }
        Ok(observed)
    }
}
#[tokio::test]
async fn comparison_is_checked_again_after_inspection_races_with_another_writer() {
    let backend = Arc::new(InspectionRace {
        fake: Fake::default(),
    });
    backend
        .fake
        .state
        .lock()
        .unwrap()
        .subjects
        .insert("stale".into(), 1);
    let p =
        AtomicPublisher::new(backend.clone(), "TEST", config(), CancellationToken::new()).unwrap();
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish(
            vec![Publication::new("stale", "a").expected_subject_sequence(0)],
            &c
        ),
        p.publish(
            vec![Publication::new("fresh", "b").expected_subject_sequence(0)],
            &c
        )
    );
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b, Err(Error::Conflict));
    assert_eq!(backend.fake.state.lock().unwrap().calls.len(), 2);
    assert_eq!(backend.fake.state.lock().unwrap().seq, 0);
}

struct FailedInspection {
    fake: Fake,
    hang: bool,
}
#[async_trait]
impl Backend for FailedInspection {
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.fake.atomic(s, m).await
    }
    async fn last_subject_sequence(&self, _: &str, _: &str) -> Result<u64, Error> {
        if self.hang {
            futures::future::pending().await
        } else {
            Err(Error::Unavailable)
        }
    }
}
#[tokio::test]
async fn failed_or_timed_out_inspection_never_authorizes_replay() {
    for hang in [false, true] {
        let backend = Arc::new(FailedInspection {
            fake: Fake::default(),
            hang,
        });
        backend.fake.state.lock().unwrap().unknown_conflict = true;
        let p = AtomicPublisher::new(
            backend.clone(),
            "TEST",
            Config {
                inspection_timeout: Duration::from_millis(10),
                ..config()
            },
            CancellationToken::new(),
        )
        .unwrap();
        let c = CancellationToken::new();
        let (a, b) = tokio::join!(
            p.publish(
                vec![Publication::new("a", "1").expected_subject_sequence(0)],
                &c
            ),
            p.publish(
                vec![Publication::new("b", "2").expected_subject_sequence(0)],
                &c
            )
        );
        let expected = Error::Unavailable;
        assert_eq!(a, Err(expected));
        assert_eq!(b, Err(expected));
        assert_eq!(backend.fake.state.lock().unwrap().calls.len(), 1);
    }
}
struct DeniedAdmission {
    fake: Fake,
}
#[async_trait]
impl Backend for DeniedAdmission {
    async fn admit(&self, _: &[Publication]) -> Result<(), Error> {
        Err(Error::Rejected)
    }
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.fake.atomic(s, m).await
    }
    async fn last_subject_sequence(&self, s: &str, t: &str) -> Result<u64, Error> {
        self.fake.last_subject_sequence(s, t).await
    }
}
#[tokio::test]
async fn priority_bypasses_policy_while_ordinary_rejection_has_no_effect() {
    let backend = Arc::new(DeniedAdmission {
        fake: Fake::default(),
    });
    let p =
        AtomicPublisher::new(backend.clone(), "TEST", config(), CancellationToken::new()).unwrap();
    let c = CancellationToken::new();
    assert_eq!(
        p.publish(vec![Publication::new("ordinary", "1")], &c).await,
        Err(Error::Rejected)
    );
    assert!(
        p.publish_priority(vec![Publication::new("priority", "1")], &c)
            .await
            .is_ok()
    );
    assert_eq!(
        backend.fake.state.lock().unwrap().calls,
        vec![vec!["priority"]]
    );
}

#[tokio::test]
async fn priority_submissions_coalesce_with_correct_receipt_ranges() {
    let fake = Arc::new(Fake::default());
    let p = publisher(fake.clone(), config());
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish_priority(
            vec![Publication::new("a", "1"), Publication::new("b", "2")],
            &c
        ),
        p.publish_priority(vec![Publication::new("c", "3")], &c),
    );
    assert_eq!(
        (a.unwrap().first_sequence, b.unwrap().last_sequence),
        (1, 3)
    );
    assert_eq!(fake.state.lock().unwrap().calls.len(), 1);
}

struct AdmissionGate {
    fake: Fake,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl Backend for AdmissionGate {
    async fn admit(&self, _: &[Publication]) -> Result<(), Error> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.fake.atomic(s, m).await
    }
    async fn last_subject_sequence(&self, s: &str, t: &str) -> Result<u64, Error> {
        self.fake.last_subject_sequence(s, t).await
    }
}
fn admission_gate() -> Arc<AdmissionGate> {
    Arc::new(AdmissionGate {
        fake: Fake::default(),
        entered: Default::default(),
        release: Default::default(),
    })
}

#[tokio::test]
async fn cancellation_during_policy_wait_never_transmits_and_recovers_budget() {
    let g = admission_gate();
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            queue_bytes: 514,
            batch_window: Duration::ZERO,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let c = CancellationToken::new();
    let task = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move { p.publish(vec![Publication::new("a", "1")], &c).await }
    });
    g.entered.notified().await;
    c.cancel();
    assert_eq!(task.await.unwrap(), Err(Error::NotPublished));
    g.release.notify_one();
    let next = tokio::spawn({
        let p = p.clone();
        async move {
            p.publish(vec![Publication::new("b", "1")], &CancellationToken::new())
                .await
        }
    });
    g.entered.notified().await;
    g.release.notify_one();
    assert_eq!(next.await.unwrap().unwrap().last_sequence, 1);
    assert_eq!(g.fake.state.lock().unwrap().calls, vec![vec!["b"]]);
}

#[tokio::test]
async fn dropping_queued_future_does_not_publish_its_messages() {
    let g = admission_gate();
    let p = AtomicPublisher::new(g.clone(), "TEST", config(), CancellationToken::new()).unwrap();
    let task = tokio::spawn({
        let p = p.clone();
        async move {
            p.publish(vec![Publication::new("a", "1")], &CancellationToken::new())
                .await
        }
    });
    g.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    g.release.notify_one();
    let receipt = p
        .publish_priority(
            vec![Publication::new("barrier", "1")],
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(receipt.last_sequence, 1);
    assert_eq!(g.fake.state.lock().unwrap().calls, vec![vec!["barrier"]]);
}

#[tokio::test]
async fn expired_deadline_in_batch_window_never_transmits() {
    let fake = Arc::new(Fake::default());
    let p = publisher(
        fake.clone(),
        Config {
            batch_window: Duration::from_millis(50),
            ..config()
        },
    );
    let c = CancellationToken::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
    let (expired, live) = tokio::join!(
        p.publish_until(vec![Publication::new("expired", "1")], deadline, &c),
        p.publish(vec![Publication::new("live", "1")], &c),
    );
    assert_eq!(expired, Err(Error::NotPublished));
    assert_eq!(live.unwrap().first_sequence, 1);
    assert_eq!(fake.state.lock().unwrap().calls, vec![vec!["live"]]);
    assert_eq!(
        p.publish_until(
            vec![Publication::new("past", "1")],
            tokio::time::Instant::now(),
            &c
        )
        .await,
        Err(Error::NotAdmitted)
    );
}

#[tokio::test]
async fn backend_policy_timeout_is_definite_nonpublication() {
    let g = admission_gate();
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            request_timeout: Duration::from_millis(10),
            batch_window: Duration::ZERO,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        p.publish_until(
            vec![Publication::new("a", "1")],
            tokio::time::Instant::now() + Duration::from_secs(1),
            &CancellationToken::new()
        )
        .await,
        Err(Error::NotPublished)
    );
    assert!(g.fake.state.lock().unwrap().calls.is_empty());
}

struct BlockedInspection {
    fake: Fake,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Backend for BlockedInspection {
    async fn atomic(&self, s: &str, m: Vec<Publication>) -> Result<u64, Error> {
        self.fake.atomic(s, m).await
    }
    async fn last_subject_sequence(&self, s: &str, t: &str) -> Result<u64, Error> {
        use std::sync::atomic::Ordering::SeqCst;
        let n = self.active.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(n, SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        self.active.fetch_sub(1, SeqCst);
        self.fake.last_subject_sequence(s, t).await
    }
}
#[tokio::test]
async fn inspection_does_not_block_writes_and_has_an_independent_bound() {
    use std::sync::atomic::Ordering::SeqCst;
    let g = Arc::new(BlockedInspection {
        fake: Fake::default(),
        entered: Default::default(),
        release: tokio::sync::Semaphore::new(0),
        active: Default::default(),
        peak: Default::default(),
    });
    for i in 0..3 {
        g.fake
            .state
            .lock()
            .unwrap()
            .subjects
            .insert(format!("stale{i}"), 99);
    }
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            max_batch_messages: 2,
            inspection_concurrency: 1,
            max_inspections: 1,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let work = tokio::spawn({
        let p = p.clone();
        async move {
            futures::future::join_all((0..6).map(|i| {
                let p = p.clone();
                async move {
                    let subject = if i % 2 == 0 {
                        format!("stale{}", i / 2)
                    } else {
                        format!("fresh{}", i / 2)
                    };
                    p.publish(
                        vec![Publication::new(subject, "1").expected_subject_sequence(0)],
                        &CancellationToken::new(),
                    )
                    .await
                }
            }))
            .await
        }
    });
    g.entered.notified().await;
    // A single write slot is enough to keep publishing while inspection is blocked.
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        p.publish_priority(
            vec![Publication::new("independent", "1")],
            &CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.messages, 1);
    assert_eq!(g.peak.load(SeqCst), 1);
    g.release.add_permits(6);
    let results = work.await.unwrap();
    for (i, result) in results.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(result, Err(Error::Conflict));
        } else {
            assert!(result.is_ok());
        }
    }
    assert_eq!(g.peak.load(SeqCst), 1);
}

#[tokio::test]
async fn priority_conflict_retry_still_bypasses_admission_policy() {
    let g = Arc::new(DeniedAdmission {
        fake: Fake::default(),
    });
    g.fake
        .state
        .lock()
        .unwrap()
        .subjects
        .insert("stale".into(), 99);
    let p = AtomicPublisher::new(g.clone(), "TEST", config(), CancellationToken::new()).unwrap();
    let c = CancellationToken::new();
    let (a, b) = tokio::join!(
        p.publish_priority(
            vec![Publication::new("stale", "1").expected_subject_sequence(0)],
            &c
        ),
        p.publish_priority(
            vec![Publication::new("fresh", "1").expected_subject_sequence(0)],
            &c
        ),
    );
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b.unwrap().last_sequence, 1);
    assert_eq!(g.fake.state.lock().unwrap().calls.len(), 2);
}

#[tokio::test]
async fn shutdown_during_policy_wait_is_definite_nonpublication() {
    let g = admission_gate();
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(g.clone(), "TEST", config(), shutdown.clone()).unwrap();
    let task = tokio::spawn(async move {
        p.publish(vec![Publication::new("a", "1")], &CancellationToken::new())
            .await
    });
    g.entered.notified().await;
    shutdown.cancel();
    assert_eq!(task.await.unwrap(), Err(Error::NotPublished));
    assert!(g.fake.state.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn cancellation_during_inspection_prevents_retry() {
    let g = Arc::new(BlockedInspection {
        fake: Fake::default(),
        entered: Default::default(),
        release: tokio::sync::Semaphore::new(0),
        active: Default::default(),
        peak: Default::default(),
    });
    g.fake
        .state
        .lock()
        .unwrap()
        .subjects
        .insert("stale".into(), 99);
    let p = AtomicPublisher::new(g.clone(), "TEST", config(), CancellationToken::new()).unwrap();
    let c = CancellationToken::new();
    let task = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move {
            let keep = CancellationToken::new();
            tokio::join!(
                p.publish(
                    vec![Publication::new("stale", "1").expected_subject_sequence(0)],
                    &keep
                ),
                p.publish(
                    vec![Publication::new("fresh", "1").expected_subject_sequence(0)],
                    &c
                ),
            )
        }
    });
    g.entered.notified().await;
    c.cancel();
    // A cancelled caller can finish without waiting for the inspection reads.
    tokio::task::yield_now().await;
    g.release.add_permits(2);
    let (a, b) = task.await.unwrap();
    assert_eq!(a, Err(Error::Conflict));
    assert_eq!(b, Err(Error::NotPublished));
    assert_eq!(g.fake.state.lock().unwrap().calls.len(), 1);
    assert!(
        p.publish(
            vec![Publication::new("barrier", "1")],
            &CancellationToken::new()
        )
        .await
        .is_ok()
    );
    assert!(!g.fake.state.lock().unwrap().subjects.contains_key("fresh"));
}

#[tokio::test]
async fn queued_cancellation_behind_busy_transport_never_publishes() {
    let g = Arc::new(Gate {
        calls: Default::default(),
        entered: Default::default(),
        release: Default::default(),
    });
    let p = AtomicPublisher::new(
        g.clone(),
        "TEST",
        Config {
            batch_window: Duration::ZERO,
            ..config()
        },
        CancellationToken::new(),
    )
    .unwrap();
    let first = tokio::spawn({
        let p = p.clone();
        async move {
            p.publish(
                vec![Publication::new("first", "1")],
                &CancellationToken::new(),
            )
            .await
        }
    });
    g.entered.notified().await;
    let c = CancellationToken::new();
    let pending = p.publish(vec![Publication::new("cancelled", "1")], &c);
    tokio::pin!(pending);
    tokio::select! { biased; r=&mut pending => panic!("unexpected completion: {r:?}"), _=tokio::task::yield_now()=>{} }
    c.cancel();
    assert_eq!(pending.await, Err(Error::NotPublished));
    g.release.notify_one();
    first.await.unwrap().unwrap();
    p.publish(
        vec![Publication::new("barrier", "1")],
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(*g.calls.lock().unwrap(), ["first", "barrier"]);
}
