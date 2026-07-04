//! Periodic heartbeat publisher.
//!
//! Publishes JSON `{"status":"connected"}` to `{base}/{machine}/heartbeat` every
//! 1 second — the same as the telemetry default `poll_interval_ms`, so liveness
//! is detected at the same granularity as sensor data.
//! The LWT (set in `mqtt::build_client`) publishes `{"status":"disconnected"}`
//! automatically if the client disconnects abnormally — we do not handle
//! disconnect publishing here.

use rumqttc::{AsyncClient, QoS};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Heartbeat interval — 1s, the same as the telemetry default `poll_interval_ms`,
/// so consumers get a liveness signal at the same granularity.
/// Promote to an env var if a different per-machine cadence is ever needed.
const INTERVAL: Duration = Duration::from_secs(1);

/// "Alive" payload. A static byte literal so there is no serde overhead per
/// publish. Content follows the payload contract (`{ status }`).
const PAYLOAD_CONNECTED: &[u8] = br#"{"status":"connected"}"#;

/// Publisher loop. Never returns — the caller embeds it in a `tokio::select!`
/// with a shutdown signal so it can exit cleanly.
///
/// `topics` = one heartbeat topic per machine (location/name) served by this
/// instance. Multi-PLC/slave in a single instance → one heartbeat per machine
/// so the dashboard knows the status of each machine. The LWT (in
/// `mqtt::build_client`) still only covers the primary machine_id; other
/// machines rely on consumer-side staleness detection when the instance dies.
pub async fn run(client: AsyncClient, topics: Vec<String>, qos: QoS) {
    info!(
        machines = topics.len(),
        ?topics,
        "heartbeat publisher starting"
    );
    let mut ticker = tokio::time::interval(INTERVAL);
    // Delay = if a tick is missed (e.g. slow publish), don't pile up a burst —
    // wait until the next interval is back to normal.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Publish once immediately — if the broker is not connected yet, rumqttc
    // queues it. Goal: consumers get a "machine online" signal at startup right
    // away instead of waiting for the first interval (1 second).
    for topic in &topics {
        publish_once(&client, topic, qos).await;
    }

    loop {
        ticker.tick().await;
        for topic in &topics {
            publish_once(&client, topic, qos).await;
        }
    }
}

async fn publish_once(client: &AsyncClient, topic: &str, qos: QoS) {
    // retain=true so a new subscriber immediately gets the last status without
    // waiting for the next heartbeat.
    match client.publish(topic, qos, true, PAYLOAD_CONNECTED).await {
        Ok(()) => debug!(%topic, "heartbeat published"),
        Err(e) => warn!(%topic, "heartbeat publish failed: {e:#}"),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_valid_json() {
        let v: serde_json::Value = serde_json::from_slice(PAYLOAD_CONNECTED).unwrap();
        assert_eq!(v["status"], "connected");
    }

    #[test]
    fn payload_matches_consumer_contract() {
        // Defense: contract test. The consumer expects `{ status: String }`
        // with status `"connected"` or `"disconnected"`. If the edge payload
        // changes shape, this test fails first — not a runtime mystery in
        // production.
        #[derive(serde::Deserialize)]
        struct ConsumerContract {
            status: String,
        }
        let parsed: ConsumerContract = serde_json::from_slice(PAYLOAD_CONNECTED).unwrap();
        assert_eq!(parsed.status, "connected");
    }
}
