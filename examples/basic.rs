use nats_atomic_publisher::{AtomicPublisher, CancellationToken, Config, NatsBackend, Publication};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = async_nats::connect(&url).await?;
    let inspection_client = async_nats::connect(&url).await?;
    let js = async_nats::jetstream::new(client.clone());
    let stream = format!("EXAMPLE_{}", uuid::Uuid::now_v7().simple());
    let subject = format!("{stream}.event");
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![format!("{stream}.>")],
        allow_atomic_publish: true,
        allow_direct: true,
        ..Default::default()
    })
    .await?;
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(NatsBackend::with_clients(
            client,
            inspection_client,
            Duration::from_secs(3),
        )?),
        &stream,
        Config::default(),
        shutdown.clone(),
    )?;
    let result = p
        .publish_until(
            vec![Publication::new(subject, "hello").expected_subject_sequence(0)],
            tokio::time::Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await;
    println!("Publication result: {result:?}");
    shutdown.cancel();
    js.delete_stream(stream).await?;
    result?;
    Ok(())
}
