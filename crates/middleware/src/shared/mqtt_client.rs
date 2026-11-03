//! MQTT publish abstraction.
//!
//! Feature dispatcher publish ke `dyn MqttPublisher`. Production injects
//! `RumqttcPublisher` (wrapper tipis di rumqttc::AsyncClient). Test inject
//! mock yang record call ke `Vec`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use rumqttc::{AsyncClient, QoS};

#[async_trait]
pub trait MqttPublisher: Send + Sync {
    /// Publish payload ke topic dengan QoS yang ditentukan. `retain=true`
    /// menyuruh broker simpan payload sebagai last-value topic (untuk status
    /// topic seperti heartbeat). Command/set/ack → `retain=false`. Mengembalikan
    /// Ok setelah broker acknowledge (untuk QoS 1+) atau immediately (QoS 0).
    async fn publish(&self, topic: &str, payload: &[u8], qos: i32, retain: bool) -> Result<()>;
}

/// Production impl yang membungkus rumqttc AsyncClient.
/// AsyncClient itu sendiri sudah `Arc<inner>` jadi cheap-clone.
#[derive(Clone)]
pub struct RumqttcPublisher {
    client: AsyncClient,
}

impl RumqttcPublisher {
    pub fn new(client: AsyncClient) -> Self {
        Self { client }
    }
}

fn to_qos(qos: i32) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

#[async_trait]
impl MqttPublisher for RumqttcPublisher {
    async fn publish(&self, topic: &str, payload: &[u8], qos: i32, retain: bool) -> Result<()> {
        self.client
            .publish(topic, to_qos(qos), retain, payload.to_vec())
            .await
            .with_context(|| format!("publish to {topic}"))?;
        Ok(())
    }
}
