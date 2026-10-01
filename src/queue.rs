//! RabbitMQ integration.
//!
//! Topology: two durable **quorum** queues (`csv.imports`, `csv.reports`) bound to the default
//! exchange. Quorum queues are RabbitMQ's replicated, crash-safe queue type. Messages are
//! persistent and published with publisher confirms, consumed with manual acks and a bounded
//! prefetch.
//!
//! The message is only a pointer (`{"job_id": ..., "kind": ...}`); all state lives in Postgres.

use std::time::Duration;

use async_trait::async_trait;
use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::jobs::JobKind;

pub const IMPORT_QUEUE: &str = "csv.imports";
pub const REPORT_QUEUE: &str = "csv.reports";

pub fn queue_for(kind: JobKind) -> &'static str {
    match kind {
        JobKind::Import => IMPORT_QUEUE,
        JobKind::Report => REPORT_QUEUE,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobMessage {
    pub job_id: Uuid,
    pub kind: JobKind,
}

/// Abstraction over "notify workers that a job is ready", so the API can be tested
/// without a broker and the broker could be swapped.
#[async_trait]
pub trait JobPublisher: Send + Sync {
    async fn publish(&self, kind: JobKind, job_id: Uuid) -> anyhow::Result<()>;
    async fn is_healthy(&self) -> bool {
        true
    }
}

/// A publisher that drops messages. Used in tests; the dispatcher would pick jobs up anyway.
pub struct NoopPublisher;

#[async_trait]
impl JobPublisher for NoopPublisher {
    async fn publish(&self, _kind: JobKind, _job_id: Uuid) -> anyhow::Result<()> {
        Ok(())
    }
}

pub async fn connect(amqp_url: &str) -> anyhow::Result<Connection> {
    let props = ConnectionProperties::default().with_connection_name("csv-reports".into());
    Ok(Connection::connect(amqp_url, props).await?)
}

/// Connect, retrying with backoff (the broker may start after us or restart).
pub async fn connect_with_retry(amqp_url: &str) -> anyhow::Result<Connection> {
    let mut delay = Duration::from_millis(500);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match connect(amqp_url).await {
            Ok(c) => {
                tracing::info!(attempt, "connected to rabbitmq");
                return Ok(c);
            }
            Err(e) if attempt < 60 => {
                tracing::warn!(attempt, error = %e, retry_in_ms = delay.as_millis() as u64, "rabbitmq not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Declare both queues (idempotent).
pub async fn declare_topology(channel: &Channel) -> anyhow::Result<()> {
    let mut args = FieldTable::default();
    args.insert("x-queue-type".into(), AMQPValue::LongString("quorum".into()));
    for q in [IMPORT_QUEUE, REPORT_QUEUE] {
        channel.queue_declare(q, QueueDeclareOptions { durable: true, ..Default::default() }, args.clone()).await?;
    }
    Ok(())
}

/// Publisher that lazily (re)connects. A publish failure drops the connection so the next
/// call reconnects; the caller treats failures as non-fatal because the dispatcher retries.
pub struct RabbitPublisher {
    url: String,
    state: Mutex<Option<(Connection, Channel)>>,
}

impl RabbitPublisher {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), state: Mutex::new(None) }
    }

    async fn channel(&self, state: &mut Option<(Connection, Channel)>) -> anyhow::Result<Channel> {
        if let Some((conn, ch)) = state.as_ref() {
            if conn.status().connected() && ch.status().connected() {
                return Ok(ch.clone());
            }
        }
        let conn = connect(&self.url).await?;
        let ch = conn.create_channel().await?;
        ch.confirm_select(ConfirmSelectOptions::default()).await?;
        declare_topology(&ch).await?;
        *state = Some((conn, ch.clone()));
        Ok(ch)
    }
}

#[async_trait]
impl JobPublisher for RabbitPublisher {
    async fn publish(&self, kind: JobKind, job_id: Uuid) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let result = async {
            let ch = self.channel(&mut state).await?;
            let body = serde_json::to_vec(&JobMessage { job_id, kind })?;
            let props = BasicProperties::default()
                .with_delivery_mode(2) // persistent
                .with_content_type("application/json".into())
                .with_message_id(job_id.to_string().into());
            let confirm = tokio::time::timeout(Duration::from_secs(5), async {
                ch.basic_publish("", queue_for(kind), BasicPublishOptions::default(), &body, props).await?.await
            })
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for publisher confirm"))??;
            match confirm {
                Confirmation::Ack(_) => Ok(()),
                other => Err(anyhow::anyhow!("broker did not ack message: {other:?}")),
            }
        }
        .await;
        if result.is_err() {
            *state = None; // force reconnect next time
        }
        result
    }

    async fn is_healthy(&self) -> bool {
        let mut state = self.state.lock().await;
        self.channel(&mut state).await.is_ok()
    }
}
