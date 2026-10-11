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
async fn timeout_after_admission_is_uncertain() {
    let p = AtomicPublisher::new(
        Arc::new(Slow),
        "TEST",
        Config {
            request_timeout: Duration::from_millis(10),
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
    let done = tokio::spawn({
        let p = p.clone();
        let c = c.clone();
        async move {
            p.publish_priority(vec![Publication::new("done", "1")], &c)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    g.release.notify_one();
    a.await.unwrap().unwrap();
    done.await.unwrap().unwrap();
    b.await.unwrap().unwrap();
    assert_eq!(*g.calls.lock().unwrap(), ["a", "done", "b"]);
}
