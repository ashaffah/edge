//! MQTT publish abstraction.
//!
//! Features publish through `dyn MqttPublisher`. Production injects
//! `RumqttcPublisher` (a thin wrapper over rumqttc::AsyncClient). Tests
//! inject a mock that records calls in a `Vec`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use rumqttc::{AsyncClient, QoS};

#[async_trait]
pub trait MqttPublisher: Send + Sync {
    /// Publish a payload to a topic with the given QoS. `retain=true` makes the
    /// broker keep it as the topic's last value (for status topics such as
    /// heartbeat). Command, set and ack use `retain=false`. Returns once the
    /// request is queued on the client.
    async fn publish(&self, topic: &str, payload: &[u8], qos: i32, retain: bool) -> Result<()>;
}

/// Production implementation over rumqttc AsyncClient.
/// AsyncClient is already an `Arc` internally, so cloning it is cheap.
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
