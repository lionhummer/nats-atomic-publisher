use crate::{Error, Publication};
use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;

/// Injectable transport and admission policy. Implementations must uphold the
/// definite/uncertain error contract and return the final stream sequence.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Optional ordinary-write capacity gate. Priority writes bypass this hook.
    /// It is not a substitute for server-side storage enforcement. It must not
    /// publish or perform submission side effects: cancellation during this hook
    /// is reported as definite non-publication.
    async fn admit(&self, _messages: &[Publication]) -> Result<(), Error> {
        Ok(())
    }
    async fn atomic(&self, stream: &str, messages: Vec<Publication>) -> Result<u64, Error>;
    async fn last_subject_sequence(&self, stream: &str, subject: &str) -> Result<u64, Error>;
}

/// Wire-protocol adapter for an existing authenticated async-nats connection.
#[derive(Clone)]
pub struct NatsBackend {
    client: async_nats::Client,
    context: async_nats::jetstream::Context,
    timeout: Duration,
}
impl NatsBackend {
    pub fn new(client: async_nats::Client, timeout: Duration) -> Result<Self, Error> {
        Self::with_clients(client.clone(), client, timeout)
    }
    /// Use independently connected clients for writes and conflict reads.
    /// Cloning a client does not create a separate connection. Both clients must
    /// access the same account and stream, with appropriate permissions.
    pub fn with_clients(
        publication_client: async_nats::Client,
        inspection_client: async_nats::Client,
        timeout: Duration,
    ) -> Result<Self, Error> {
        if timeout.is_zero() || tokio::time::Instant::now().checked_add(timeout).is_none() {
            return Err(Error::Invalid);
        }
        Ok(Self {
            context: async_nats::jetstream::new(inspection_client),
            client: publication_client,
            timeout,
        })
    }
}
#[derive(Deserialize)]
struct Ack {
    stream: String,
    seq: u64,
    batch: String,
    count: usize,
}
#[derive(Deserialize)]
struct Rejection {
    error: ServerError,
}
#[derive(Deserialize)]
struct ServerError {
    err_code: u64,
}
fn rejection(payload: &[u8]) -> Option<Error> {
    serde_json::from_slice::<Rejection>(payload).ok().map(|r| {
        if r.error.err_code == 10071 {
            Error::Conflict
        } else {
            Error::Rejected
        }
    })
}
#[async_trait]
impl Backend for NatsBackend {
    async fn atomic(&self, stream: &str, messages: Vec<Publication>) -> Result<u64, Error> {
        crate::validate_messages(stream, &messages)?;
        let count = messages.len();
        let batch = uuid::Uuid::now_v7().to_string();
        let send = async {
            for (index, mut message) in messages.into_iter().enumerate() {
                message.headers.insert("Nats-Batch-Id", batch.as_str());
                message
                    .headers
                    .insert("Nats-Batch-Sequence", (index + 1).to_string());
                message.headers.insert("Nats-Expected-Stream", stream);
                if index == 0 && count > 1 {
                    let ack = self
                        .client
                        .request_with_headers(message.subject, message.headers, message.payload)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    if let Some(error) = rejection(&ack.payload) {
                        return Err(error);
                    }
                    if !ack.payload.is_empty() {
                        return Err(Error::InvalidReceipt);
                    }
                } else if index + 1 == count {
                    message.headers.insert("Nats-Batch-Commit", "1");
                    let ack = self
                        .client
                        .request_with_headers(message.subject, message.headers, message.payload)
                        .await
                        .map_err(|_| Error::Indeterminate)?;
                    if let Some(error) = rejection(&ack.payload) {
                        return Err(error);
                    }
                    let ack: Ack =
                        serde_json::from_slice(&ack.payload).map_err(|_| Error::InvalidReceipt)?;
                    if ack.stream != stream
                        || ack.batch != batch
                        || ack.count != count
                        || ack.seq < count as u64
                    {
                        return Err(Error::InvalidReceipt);
                    }
                    return Ok(ack.seq);
                } else {
                    self.client
                        .publish_with_headers(message.subject, message.headers, message.payload)
                        .await
                        .map_err(|_| Error::Indeterminate)?;
                }
            }
            Err(Error::InvalidReceipt)
        };
        tokio::time::timeout(self.timeout, send)
            .await
            .map_err(|_| Error::Indeterminate)?
    }
    async fn last_subject_sequence(&self, name: &str, subject: &str) -> Result<u64, Error> {
        let read = async {
            let stream = self
                .context
                .get_stream_no_info(name)
                .await
                .map_err(|_| Error::Unavailable)?;
            match stream.direct_get_last_for_subject(subject).await {
                Ok(message) => Ok(message.sequence),
                Err(error)
                    if error.kind()
                        == async_nats::jetstream::stream::DirectGetErrorKind::NotFound =>
                {
                    Ok(0)
                }
                Err(_) => Err(Error::Unavailable),
            }
        };
        tokio::time::timeout(self.timeout, read)
            .await
            .map_err(|_| Error::Unavailable)?
    }
}
