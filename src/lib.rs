#![doc = include_str!("../README.md")]
//! Concurrent atomic publishing for a single NATS JetStream stream.
//!
//! Each submission is indivisible. Independent submissions can share a batch.
//! Only a definite conflict permits isolation and resubmission; uncertain writes
//! are never automatically retried. See the repository README for reconciliation.
mod backend;
mod publisher;
pub use async_nats::HeaderMap;
pub use backend::{Backend, NatsBackend};
use bytes::Bytes;
pub use publisher::{AtomicPublisher, Config};
pub use tokio_util::sync::CancellationToken;

/// A message in an indivisible submission.
#[derive(Clone, Debug)]
pub struct Publication {
    pub subject: String,
    pub headers: HeaderMap,
    pub payload: Bytes,
}
impl Publication {
    pub fn new(subject: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            subject: subject.into(),
            headers: HeaderMap::new(),
            payload: payload.into(),
        }
    }
    /// Compare against the last stream sequence on this subject; zero means absent.
    pub fn expected_subject_sequence(mut self, sequence: u64) -> Self {
        self.headers
            .insert("Nats-Expected-Last-Subject-Sequence", sequence.to_string());
        self
    }
    pub(crate) fn bytes(&self) -> Option<usize> {
        self.headers.iter().try_fold(
            self.payload
                .len()
                .checked_add(self.subject.len())?
                .checked_add(512)?,
            |n, (key, values)| {
                values.iter().try_fold(n, |n, value| {
                    n.checked_add(AsRef::<str>::as_ref(key).len())?
                        .checked_add(value.as_str().len())
                })
            },
        )
    }
}
/// Final sequence of this submission, not of an unrelated submission sharing its batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub messages: usize,
}

/// Errors intentionally distinguish definite non-admission from uncertain effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration, message, or submission limit")]
    Invalid,
    #[error("publisher closed before admission")]
    Closed,
    #[error("cancelled or timed out before admission; no publication was queued")]
    NotAdmitted,
    #[error("publish outcome is uncertain; reconcile before retrying")]
    Indeterminate,
    #[error("server rejected an optimistic concurrency comparison")]
    Conflict,
    #[error("server definitely rejected the batch")]
    Rejected,
    #[error("broker unavailable before commit or during conflict inspection")]
    Unavailable,
    #[error("invalid commit receipt; reconcile before retrying")]
    InvalidReceipt,
}
impl Error {
    /// An uncertain error cannot authorize retrying the same side effect.
    pub fn is_uncertain(self) -> bool {
        matches!(self, Self::Indeterminate | Self::InvalidReceipt)
    }
}

// Shared by queued and direct transports: reject ambiguous comparisons before IO.
pub(crate) fn valid_stream(stream: &str) -> bool {
    !stream.is_empty()
        && !stream
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || ".*>/\\".contains(c))
}
pub(crate) fn validate_messages(stream: &str, messages: &[Publication]) -> Result<(), Error> {
    if !valid_stream(stream) || messages.is_empty() {
        return Err(Error::Invalid);
    }
    let mut subjects = std::collections::HashSet::new();
    for (index, m) in messages.iter().enumerate() {
        if m.subject.split('.').any(|s| {
            s.is_empty()
                || s.chars()
                    .any(|c| c.is_whitespace() || c.is_control() || "*>".contains(c))
        }) {
            return Err(Error::Invalid);
        }
        for h in ["Nats-Batch-Id", "Nats-Batch-Sequence", "Nats-Batch-Commit"] {
            if m.headers.get(h).is_some() {
                return Err(Error::Invalid);
            }
        }
        for h in [
            "Nats-Expected-Last-Subject-Sequence",
            "Nats-Expected-Last-Sequence",
            "Nats-Expected-Stream",
            "Nats-Expected-Last-Subject-Sequence-Subject",
        ] {
            if m.headers.get_all(h).count() > 1 {
                return Err(Error::Invalid);
            }
        }
        for h in [
            "Nats-Expected-Last-Subject-Sequence",
            "Nats-Expected-Last-Sequence",
        ] {
            if let Some(value) = m.headers.get(h) {
                value.as_str().parse::<u64>().map_err(|_| Error::Invalid)?;
            }
        }
        if m.headers
            .get("Nats-Expected-Stream")
            .is_some_and(|v| v.as_str() != stream)
            || (index > 0 && m.headers.get("Nats-Expected-Last-Sequence").is_some())
            || (subjects.contains(&m.subject)
                && m.headers
                    .get("Nats-Expected-Last-Subject-Sequence")
                    .is_some())
        {
            return Err(Error::Invalid);
        }
        subjects.insert(&m.subject);
    }
    Ok(())
}
