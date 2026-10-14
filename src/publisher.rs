use crate::{Backend, CancellationToken, Error, Publication, Receipt};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

/// Per-publisher limits, independent of the server's operator-configured limits.
#[derive(Clone, Debug)]
pub struct Config {
    pub queue_capacity: usize,
    pub priority_queue_capacity: usize,
    pub queue_bytes: usize,
    pub priority_queue_bytes: usize,
    pub max_batch_messages: usize,
    pub max_batch_bytes: usize,
    pub max_in_flight: usize,
    pub batch_window: Duration,
    pub request_timeout: Duration,
    pub inspection_timeout: Duration,
    pub inspection_concurrency: usize,
    /// Maximum concurrent conflict-inspection groups, separate from writes.
    pub max_inspections: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            queue_capacity: 2048,
            priority_queue_capacity: 256,
            queue_bytes: 64 * 1024 * 1024,
            priority_queue_bytes: 8 * 1024 * 1024,
            max_batch_messages: 1000,
            max_batch_bytes: 4 * 1024 * 1024,
            max_in_flight: 2,
            batch_window: Duration::from_millis(1),
            request_timeout: Duration::from_secs(5),
            inspection_timeout: Duration::from_secs(1),
            inspection_concurrency: 16,
            max_inspections: 2,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if [
            self.queue_capacity,
            self.priority_queue_capacity,
            self.queue_bytes,
            self.priority_queue_bytes,
            self.max_batch_messages,
            self.max_batch_bytes,
            self.max_in_flight,
            self.inspection_concurrency,
            self.max_inspections,
        ]
        .contains(&0)
            || self.queue_bytes > u32::MAX as usize
            || self.priority_queue_bytes > u32::MAX as usize
            || self.queue_capacity > Semaphore::MAX_PERMITS
            || self.priority_queue_capacity > Semaphore::MAX_PERMITS
            || self.queue_bytes > Semaphore::MAX_PERMITS
            || self.priority_queue_bytes > Semaphore::MAX_PERMITS
            || self.request_timeout.is_zero()
            || self.inspection_timeout.is_zero()
            || [
                self.request_timeout,
                self.inspection_timeout,
                self.batch_window,
            ]
            .iter()
            .any(|d| tokio::time::Instant::now().checked_add(*d).is_none())
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct AtomicPublisher {
    ordinary: mpsc::Sender<Pending>,
    priority: mpsc::Sender<Pending>,
    ordinary_budget: Arc<Semaphore>,
    priority_budget: Arc<Semaphore>,
    config: Arc<Config>,
    shutdown: CancellationToken,
    stream: String,
}
const QUEUED: u8 = 0;
const PUBLISHING: u8 = 1;
const ABANDONED: u8 = 2;
struct WaitGuard(Arc<AtomicU8>);
impl WaitGuard {
    fn abandon(&self) -> Error {
        match self
            .0
            .compare_exchange(QUEUED, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(ABANDONED) => Error::NotPublished,
            Err(_) => Error::Indeterminate,
        }
    }
}
impl Drop for WaitGuard {
    fn drop(&mut self) {
        let _ = self.abandon();
    }
}
struct Pending {
    messages: Vec<Publication>,
    deadline: tokio::time::Instant,
    phase: Arc<AtomicU8>,
    bytes: usize,
    reply: oneshot::Sender<Result<Receipt, Error>>,
    _permit: OwnedSemaphorePermit,
}
impl Pending {
    fn live(&self) -> bool {
        if self.reply.is_closed() || tokio::time::Instant::now() >= self.deadline {
            let _ =
                self.phase
                    .compare_exchange(QUEUED, ABANDONED, Ordering::AcqRel, Ordering::Acquire);
        }
        self.phase.load(Ordering::Acquire) == QUEUED
    }
}
fn retain_live(group: &mut Vec<Pending>) {
    let pending = std::mem::take(group);
    for p in pending {
        if p.live() {
            group.push(p);
        } else {
            let _ = p.reply.send(Err(Error::NotPublished));
        }
    }
}
impl AtomicPublisher {
    /// Must be constructed inside a Tokio runtime. Dropping all handles drains
    /// live admitted work; cancelling shutdown abandons queued work. Writes that
    /// have started transmission still have uncertain outcomes.
    pub fn new(
        backend: Arc<dyn Backend>,
        stream: impl Into<String>,
        config: Config,
        shutdown: CancellationToken,
    ) -> Result<Self, Error> {
        config.validate()?;
        let stream = stream.into();
        if !crate::valid_stream(&stream) {
            return Err(Error::Invalid);
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|_| Error::Invalid)?;
        let (ordinary, ordinary_rx) = mpsc::channel(config.queue_capacity);
        let (priority, priority_rx) = mpsc::channel(config.priority_queue_capacity);
        let config = Arc::new(config);
        handle.spawn(run(
            backend,
            stream.clone(),
            config.clone(),
            ordinary_rx,
            priority_rx,
            shutdown.clone(),
        ));
        Ok(Self {
            ordinary,
            priority,
            ordinary_budget: Arc::new(Semaphore::new(config.queue_bytes)),
            priority_budget: Arc::new(Semaphore::new(config.priority_queue_bytes)),
            config,
            shutdown,
            stream,
        })
    }
    /// Publish an indivisible ordinary submission.
    pub async fn publish(
        &self,
        messages: Vec<Publication>,
        cancellation: &CancellationToken,
    ) -> Result<Receipt, Error> {
        self.publish_until(
            messages,
            tokio::time::Instant::now() + self.config.request_timeout,
            cancellation,
        )
        .await
    }
    /// Separate byte/queue budget and scheduling priority. Does not reserve
    /// server disk or preempt a batch already in flight. May starve ordinary work.
    pub async fn publish_priority(
        &self,
        messages: Vec<Publication>,
        cancellation: &CancellationToken,
    ) -> Result<Receipt, Error> {
        self.publish_priority_until(
            messages,
            tokio::time::Instant::now() + self.config.request_timeout,
            cancellation,
        )
        .await
    }
    /// Publish with an absolute caller deadline, including capacity and queue wait.
    /// The configured request timeout still bounds each backend write attempt.
    pub async fn publish_until(
        &self,
        messages: Vec<Publication>,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Receipt, Error> {
        self.enqueue(messages, false, deadline, cancellation).await
    }
    /// Priority variant of [`Self::publish_until`].
    pub async fn publish_priority_until(
        &self,
        messages: Vec<Publication>,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Receipt, Error> {
        self.enqueue(messages, true, deadline, cancellation).await
    }
    async fn enqueue(
        &self,
        messages: Vec<Publication>,
        priority: bool,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Receipt, Error> {
        if messages.is_empty() || messages.len() > self.config.max_batch_messages {
            return Err(Error::Invalid);
        }
        crate::validate_messages(&self.stream, &messages)?;
        let bytes = messages
            .iter()
            .try_fold(0usize, |n, m| n.checked_add(m.bytes()?))
            .ok_or(Error::Invalid)?;
        let (sender, budget, limit) = if priority {
            (
                &self.priority,
                &self.priority_budget,
                self.config.priority_queue_bytes,
            )
        } else {
            (
                &self.ordinary,
                &self.ordinary_budget,
                self.config.queue_bytes,
            )
        };
        if bytes > limit || bytes > self.config.max_batch_bytes {
            return Err(Error::Invalid);
        }
        if deadline <= tokio::time::Instant::now() {
            return Err(Error::NotAdmitted);
        }
        let admission = async {
            let permit = budget
                .clone()
                .acquire_many_owned(bytes as u32)
                .await
                .map_err(|_| Error::Closed)?;
            let slot = sender.reserve().await.map_err(|_| Error::Closed)?;
            Ok::<_, Error>((permit, slot))
        };
        let (permit, slot) = tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(Error::NotAdmitted),
            _ = self.shutdown.cancelled() => return Err(Error::Closed),
            result = tokio::time::timeout_at(deadline, admission) => result.map_err(|_| Error::NotAdmitted)??,
        };
        let (reply, receipt) = oneshot::channel();
        let guard = WaitGuard(Arc::new(AtomicU8::new(QUEUED)));
        slot.send(Pending {
            deadline,
            phase: guard.0.clone(),
            messages,
            bytes,
            reply,
            _permit: permit,
        });
        tokio::select! { biased;
            result = receipt => result.map_err(|_| guard.abandon())?,
            _ = cancellation.cancelled() => Err(guard.abandon()),
            _ = self.shutdown.cancelled() => Err(guard.abandon()),
            _ = tokio::time::sleep_until(deadline) => Err(guard.abandon()),
        }
    }
}
// Global sequence/message-id comparisons must remain independent: merging can
// change their meaning. Subject writes that overlap are never coalesced either.
fn exclusive(p: &Pending) -> bool {
    p.messages.iter().any(|m| {
        [
            "Nats-Expected-Last-Sequence",
            "Nats-Expected-Last-Msg-Id",
            "Nats-Expected-Last-Subject-Sequence-Subject",
        ]
        .iter()
        .any(|h| m.headers.get(*h).is_some())
    })
}
fn subjects(p: &Pending) -> HashSet<&str> {
    p.messages.iter().map(|m| m.subject.as_str()).collect()
}
async fn collect(
    first: Pending,
    receiver: &mut mpsc::Receiver<Pending>,
    config: &Config,
    shutdown: &CancellationToken,
) -> (Vec<Pending>, Option<Pending>) {
    let mut count = first.messages.len();
    let mut bytes = first.bytes;
    let mut names: HashSet<String> = subjects(&first).into_iter().map(str::to_owned).collect();
    let solo = exclusive(&first);
    let mut group = vec![first];
    let until = tokio::time::Instant::now() + config.batch_window;
    while !solo && count < config.max_batch_messages && bytes < config.max_batch_bytes {
        let next = tokio::select! { biased;
            _ = shutdown.cancelled() => None,
            result = tokio::time::timeout_at(until, receiver.recv()) => result.ok().flatten(),
        };
        let Some(next) = next else {
            break;
        };
        if exclusive(&next)
            || subjects(&next).iter().any(|s| names.contains(*s))
            || count + next.messages.len() > config.max_batch_messages
            || bytes + next.bytes > config.max_batch_bytes
        {
            return (group, Some(next));
        }
        count += next.messages.len();
        bytes += next.bytes;
        names.extend(subjects(&next).into_iter().map(str::to_owned));
        group.push(next);
    }
    (group, None)
}
async fn run(
    backend: Arc<dyn Backend>,
    stream: String,
    config: Arc<Config>,
    mut ordinary: mpsc::Receiver<Pending>,
    mut priority: mpsc::Receiver<Pending>,
    shutdown: CancellationToken,
) {
    let mut active = FuturesUnordered::new();
    let mut inspections = FuturesUnordered::new();
    let mut conflicts = VecDeque::new();
    let mut retries = VecDeque::new();
    let mut deferred_ordinary = None;
    let mut deferred_priority = None;
    let mut ordinary_open = true;
    let mut priority_open = true;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if inspections.len() < config.max_inspections
            && let Some((group, lane)) = conflicts.pop_front()
        {
            inspections.push(inspect(
                backend.clone(),
                stream.clone(),
                config.clone(),
                group,
                lane,
            ));
            continue;
        }
        if active.len() < config.max_in_flight {
            if let Some(first) = deferred_priority
                .take()
                .or_else(|| priority.try_recv().ok())
            {
                let (group, next) = collect(first, &mut priority, &config, &shutdown).await;
                deferred_priority = next;
                active.push(execute(
                    backend.clone(),
                    stream.clone(),
                    config.clone(),
                    group,
                    true,
                ));
                continue;
            }
            if let Some((group, lane)) = retries.pop_front() {
                active.push(execute(
                    backend.clone(),
                    stream.clone(),
                    config.clone(),
                    group,
                    lane,
                ));
                continue;
            }
            if let Some(first) = deferred_ordinary.take() {
                let (group, next) = collect(first, &mut ordinary, &config, &shutdown).await;
                deferred_ordinary = next;
                active.push(execute(
                    backend.clone(),
                    stream.clone(),
                    config.clone(),
                    group,
                    false,
                ));
                continue;
            }
        }
        if !ordinary_open
            && !priority_open
            && active.is_empty()
            && inspections.is_empty()
            && conflicts.is_empty()
            && retries.is_empty()
            && deferred_ordinary.is_none()
            && deferred_priority.is_none()
        {
            break;
        }
        tokio::select! { biased;
            _ = shutdown.cancelled() => break,
            result = active.next(), if !active.is_empty() => {
                if let Some(Some(conflict)) = result { conflicts.push_back(conflict); }
            },
            result = inspections.next(), if !inspections.is_empty() => {
                if let Some((group, lane)) = result && !group.is_empty() { retries.push_back((group, lane)); }
            },
            pending = priority.recv(), if priority_open && active.len() < config.max_in_flight => {
                if let Some(first) = pending {
                    let (group, next) = collect(first, &mut priority, &config, &shutdown).await;
                    deferred_priority = next;
                    active.push(execute(backend.clone(), stream.clone(), config.clone(), group, true));
                } else { priority_open = false; }
            },
            pending = ordinary.recv(), if ordinary_open && active.len() < config.max_in_flight => {
                if let Some(first) = pending {
                    let (group, next) = collect(first, &mut ordinary, &config, &shutdown).await;
                    deferred_ordinary = next;
                    active.push(execute(backend.clone(), stream.clone(), config.clone(), group, false));
                } else { ordinary_open = false; }
            },
        }
    }
}
async fn execute(
    backend: Arc<dyn Backend>,
    stream: String,
    config: Arc<Config>,
    mut group: Vec<Pending>,
    priority: bool,
) -> Option<(Vec<Pending>, bool)> {
    retain_live(&mut group);
    if group.is_empty() {
        return None;
    }
    let write = async {
        if !priority {
            let messages = group
                .iter()
                .flat_map(|p| p.messages.iter().cloned())
                .collect::<Vec<_>>();
            backend.admit(&messages).await?;
        }
        retain_live(&mut group);
        group.retain(|p| {
            p.phase
                .compare_exchange(QUEUED, PUBLISHING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });
        if group.is_empty() {
            return Ok(0);
        }
        let messages = group
            .iter()
            .flat_map(|p| p.messages.iter().cloned())
            .collect();
        backend.atomic(&stream, messages).await
    };
    let result = tokio::time::timeout(config.request_timeout, write).await;
    let result = match result {
        Ok(result) => result,
        Err(_) => {
            for p in group {
                let error = WaitGuard(p.phase.clone()).abandon();
                let _ = p.reply.send(Err(error));
            }
            return None;
        }
    };
    match result {
        Ok(last) => {
            let total: usize = group.iter().map(|p| p.messages.len()).sum();
            let Some(mut position) = last.checked_sub(total as u64) else {
                fail(group, Error::InvalidReceipt);
                return None;
            };
            for p in group {
                let first_sequence = position + 1;
                position += p.messages.len() as u64;
                let _ = p.reply.send(Ok(Receipt {
                    first_sequence,
                    last_sequence: position,
                    messages: p.messages.len(),
                }));
            }
        }
        Err(Error::Conflict) if group.len() > 1 => {
            for p in &group {
                p.phase.store(QUEUED, Ordering::Release);
            }
            return Some((group, priority));
        }
        Err(error) => fail(group, error),
    }
    None
}
async fn inspect(
    backend: Arc<dyn Backend>,
    stream: String,
    config: Arc<Config>,
    mut group: Vec<Pending>,
    priority: bool,
) -> (Vec<Pending>, bool) {
    retain_live(&mut group);
    if group.is_empty() {
        return (group, priority);
    }
    let checks =
        futures::stream::iter(group.iter().map(|p| p.messages.clone()).collect::<Vec<_>>())
            .map(|messages| {
                let backend = backend.clone();
                let stream = stream.clone();
                async move {
                    for m in &messages {
                        if let Some(expected) = m.headers.get("Nats-Expected-Last-Subject-Sequence")
                        {
                            let expected = expected
                                .as_str()
                                .parse::<u64>()
                                .map_err(|_| Error::Invalid)?;
                            if backend.last_subject_sequence(&stream, &m.subject).await? != expected
                            {
                                return Err(Error::Conflict);
                            }
                        }
                    }
                    Ok(())
                }
            })
            .buffered(config.inspection_concurrency)
            .collect::<Vec<_>>();
    let results = tokio::time::timeout(config.inspection_timeout, checks).await;
    let Ok(results) = results else {
        fail(group, Error::Unavailable);
        return (Vec::new(), priority);
    };
    // Re-submit only after identifying at least one actual conflict.
    if !results.contains(&Err(Error::Conflict)) {
        for (p, result) in group.into_iter().zip(results) {
            let _ = p.reply.send(Err(result.err().unwrap_or(Error::Conflict)));
        }
        return (Vec::new(), priority);
    }
    group = group
        .into_iter()
        .zip(results)
        .filter_map(|(p, r)| match r {
            Ok(()) if !p.reply.is_closed() => Some(p),
            Ok(()) => None,
            Err(e) => {
                let _ = p.reply.send(Err(e));
                None
            }
        })
        .collect();

    (group, priority)
}
fn fail(group: Vec<Pending>, error: Error) {
    for p in group {
        let _ = p.reply.send(Err(error));
    }
}
