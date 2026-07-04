//! Periodic PLC status publisher.
//!
//! Publishes `{base}/{machine}/plc/status` every second + immediately whenever
//! the status changes. State is received from `telemetry::run` via a `watch`
//! channel — telemetry is what knows when Modbus connects/disconnects.
//!
//! Similar to the `heartbeat` pattern, but the payload is dynamic
//! (connected/disconnected) rather than always "connected".

use rumqttc::{AsyncClient, QoS};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};

const INTERVAL: Duration = Duration::from_secs(1);

const PAYLOAD_CONNECTED: &[u8] = br#"{"status":"connected"}"#;
const PAYLOAD_DISCONNECTED: &[u8] = br#"{"status":"disconnected"}"#;

/// Publisher loop. Never returns — embed it in a `tokio::select!` with a
/// shutdown signal.
///
/// Publishing is triggered by two sources:
/// - A 1-second ticker (periodic, guarantees status is sent even with no change)
/// - `status_rx.changed()` — immediately when telemetry updates the state
///   (connect/disconnect)
pub async fn run(
    client: AsyncClient,
    topic: String,
    qos: QoS,
    mut status_rx: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(INTERVAL);
    // Delay: if publishing is slow, don't burst catch-up — wait for the next
    // interval.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            // changed() returns Err if the sender is dropped (= telemetry exit),
            // the arm doesn't match → the ticker keeps handling it — no need to
            // panic.
            Ok(()) = status_rx.changed() => {}
        }
        // Borrow + deref into a local bool immediately — the guard is dropped
        // before the .await, so no RwLockReadGuard is held across the await
        // point.
        let connected = *status_rx.borrow();
        publish_once(&client, &topic, qos, connected).await;
    }
}

async fn publish_once(client: &AsyncClient, topic: &str, qos: QoS, connected: bool) {
    let payload = if connected {
        PAYLOAD_CONNECTED
    } else {
        PAYLOAD_DISCONNECTED
    };
    // retain=true so a new subscriber immediately gets the last status without
    // waiting for the next publish.
    match client.publish(topic, qos, true, payload).await {
        Ok(()) => debug!(%topic, connected, "plc status published"),
        Err(e) => warn!(%topic, "plc status publish failed: {e:#}"),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_connected_is_valid_json() {
        let v: serde_json::Value = serde_json::from_slice(PAYLOAD_CONNECTED).unwrap();
        assert_eq!(v["status"], "connected");
    }

    #[test]
    fn payload_disconnected_is_valid_json() {
        let v: serde_json::Value = serde_json::from_slice(PAYLOAD_DISCONNECTED).unwrap();
        assert_eq!(v["status"], "disconnected");
    }

    #[test]
    fn payload_matches_consumer_contract() {
        // Defense: contract test. The consumer expects `{ status: String }`.
        // If the shape changes, this test fails first — not a runtime mystery
        // in production.
        #[derive(serde::Deserialize)]
        struct ConsumerContract {
            status: String,
        }
        let connected: ConsumerContract = serde_json::from_slice(PAYLOAD_CONNECTED).unwrap();
        let disconnected: ConsumerContract = serde_json::from_slice(PAYLOAD_DISCONNECTED).unwrap();
        assert_eq!(connected.status, "connected");
        assert_eq!(disconnected.status, "disconnected");
    }
}
