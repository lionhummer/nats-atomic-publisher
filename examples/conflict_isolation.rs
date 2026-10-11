use nats_atomic_publisher::{AtomicPublisher, CancellationToken, Config, NatsBackend, Publication};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = async_nats::connect(url).await?;
    let js = async_nats::jetstream::new(client.clone());
    let name = format!("CONFLICT_{}", uuid::Uuid::now_v7().simple());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: name.clone(),
        subjects: vec![format!("{name}.>")],
        allow_atomic_publish: true,
        allow_direct: true,
        ..Default::default()
    })
    .await?;
    let shutdown = CancellationToken::new();
    let p = AtomicPublisher::new(
        Arc::new(NatsBackend::new(client, Duration::from_secs(3))?),
        &name,
        Config {
            max_in_flight: 1,
            batch_window: Duration::from_millis(10),
            ..Default::default()
        },
        shutdown.clone(),
    )?;
    let cancellation = CancellationToken::new();
    let result = async {
        p.publish(
            vec![
                Publication::new(format!("{name}.existing"), "initial")
                    .expected_subject_sequence(0),
            ],
            &cancellation,
        )
        .await?;
        let (stale, fresh) = tokio::join!(
            p.publish(
                vec![
                    Publication::new(format!("{name}.existing"), "stale")
                        .expected_subject_sequence(0)
                ],
                &cancellation
            ),
            p.publish(
                vec![
                    Publication::new(format!("{name}.independent"), "fresh")
                        .expected_subject_sequence(0)
                ],
                &cancellation
            )
        );
        println!("Stale operation: {stale:?}\nIndependent operation: {fresh:?}");
        fresh?;
        Ok::<_, nats_atomic_publisher::Error>(())
    }
    .await;
    shutdown.cancel();
    js.delete_stream(name).await?;
    result?;
    Ok(())
}
