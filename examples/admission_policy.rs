//! Wrap a real backend with an ordinary-write admission gate.
use async_trait::async_trait;
use nats_atomic_publisher::{
    AtomicPublisher, Backend, CancellationToken, Config, Error, NatsBackend, Publication,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
struct PolicyBackend {
    inner: NatsBackend,
    ordinary_open: Arc<AtomicBool>,
}
#[async_trait]
impl Backend for PolicyBackend {
    async fn admit(&self, _: &[Publication]) -> Result<(), Error> {
        if self.ordinary_open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Rejected)
        }
    }
    async fn atomic(&self, stream: &str, messages: Vec<Publication>) -> Result<u64, Error> {
        self.inner.atomic(stream, messages).await
    }
    async fn last_subject_sequence(&self, stream: &str, subject: &str) -> Result<u64, Error> {
        self.inner.last_subject_sequence(stream, subject).await
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = async_nats::connect(url).await?;
    let js = async_nats::jetstream::new(client.clone());
    let name = format!("POLICY_{}", uuid::Uuid::now_v7().simple());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: name.clone(),
        subjects: vec![format!("{name}.>")],
        allow_atomic_publish: true,
        allow_direct: true,
        ..Default::default()
    })
    .await?;
    let backend = PolicyBackend {
        inner: NatsBackend::new(client, Duration::from_secs(3))?,
        ordinary_open: Arc::new(AtomicBool::new(false)),
    };
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(backend),
        &name,
        Config::default(),
        shutdown.clone(),
    )?;
    let cancellation = CancellationToken::new();
    let ordinary = p
        .publish(
            vec![Publication::new(format!("{name}.data"), "blocked")],
            &cancellation,
        )
        .await;
    let priority = p
        .publish_priority(
            vec![Publication::new(format!("{name}.done"), "settled")],
            &cancellation,
        )
        .await;
    println!("Ordinary: {ordinary:?}\nPriority: {priority:?}");
    shutdown.cancel();
    js.delete_stream(name).await?;
    priority?;
    Ok(())
}
