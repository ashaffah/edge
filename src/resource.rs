//! Resource publisher: periodic snapshot of edge CPU + memory + IP → MQTT.
//!
//! Publishes `{"memory_pct": f64, "cpu_pct": f64, "ip": "..."}` to the topic
//! `{base}/{machine}/resources`. No range validation on the edge side —
//! dashboards/observability detect outliers.
//!
//! ## CPU sampling
//! CPU% in sysinfo is computed from the **delta** between two consecutive
//! refreshes. Without a baseline, the first sample = 0 or garbage. Solution:
//! warm up before the loop (refresh, sleep MINIMUM_CPU_UPDATE_INTERVAL, refresh
//! again). After that the 10s interval is well above the minimum and samples
//! are accurate.

use rumqttc::{AsyncClient, QoS};
use serde::Serialize;
use std::time::Duration;
use sysinfo::System;
use tracing::{debug, warn};

/// Default resource publisher interval (1s).
/// A less frequent resource snapshot is plenty for operational visibility.
const INTERVAL: Duration = Duration::from_secs(1);

/// IP fallback if detection fails (edge starting up before DHCP, etc.).
/// `"0.0.0.0"` = an explicit "unknown", not a crash.
const IP_UNKNOWN: &str = "0.0.0.0";

/// Payload contract — field names `memory_pct` / `cpu_pct` / `ip`
/// (not `memory_percent` / `ip_address`).
#[derive(Debug, Serialize)]
struct ResourcePayload<'a> {
    memory_pct: f64,
    cpu_pct: f64,
    ip: &'a str,
}

/// Publisher loop. Never returns — the caller embeds it in a `tokio::select!`.
///
/// `topics` = one resources topic per machine (location/name) served by this
/// instance. The CPU/mem/IP snapshot is computed once per tick and then
/// published to all topics (same data — one board serves many machines).
pub async fn run(client: AsyncClient, topics: Vec<String>, qos: QoS) {
    let mut sys = System::new();

    // Warm up the CPU sampler: refresh once for a baseline, sleep the minimum
    // interval, refresh once more. After that the first publish has a
    // meaningful CPU%.
    sys.refresh_cpu_usage();
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;

    let mut ticker = tokio::time::interval(INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Publish immediately so consumers get the first snapshot right away
    // (without waiting for the first 10s).
    publish_snapshot(&client, &topics, qos, &mut sys).await;

    loop {
        ticker.tick().await;
        publish_snapshot(&client, &topics, qos, &mut sys).await;
    }
}

async fn publish_snapshot(client: &AsyncClient, topics: &[String], qos: QoS, sys: &mut System) {
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let cpu_pct = sys.global_cpu_usage() as f64;
    let memory_pct = compute_memory_pct(sys.used_memory(), sys.total_memory());
    let ip = detect_ip();

    let payload = ResourcePayload {
        memory_pct,
        cpu_pct,
        ip: &ip,
    };
    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            // Practically impossible (3 simple fields), but handled explicitly
            // so the error is visible if the struct changes later.
            warn!("serialize resource payload failed: {e:#}");
            return;
        }
    };

    // retain=true, same as heartbeat: a new subscriber immediately sees the
    // last snapshot without waiting for the next interval.
    for topic in topics {
        match client.publish(topic, qos, true, bytes.clone()).await {
            Ok(()) => debug!(%topic, cpu_pct, memory_pct, %ip, "resource published"),
            Err(e) => warn!(%topic, "resource publish failed: {e:#}"),
        }
    }
}

/// Compute memory% from used/total bytes. Defensive against divide-by-zero if
/// total is 0 (system info unavailable — an edge case that should not happen
/// but must not crash).
fn compute_memory_pct(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (used as f64 / total as f64) * 100.0
    }
}

/// Detect the primary IP. The `local-ip-address` crate iterates interfaces and
/// returns the first non-loopback IPv4. Falls back to `"0.0.0.0"` on failure —
/// so the consumer still receives the publish and the dashboard interprets
/// `0.0.0.0` as "not ready".
fn detect_ip() -> String {
    match local_ip_address::local_ip() {
        Ok(ip) => ip.to_string(),
        Err(e) => {
            warn!("IP detection failed: {e:#}, fallback to {IP_UNKNOWN}");
            IP_UNKNOWN.to_string()
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_serializes_with_rust_native_field_names() {
        // Contract test: consumer expects fields `memory_pct` / `cpu_pct` / `ip`.
        let payload = ResourcePayload {
            memory_pct: 42.5,
            cpu_pct: 13.7,
            ip: "192.168.1.20",
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"memory_pct\":42.5"));
        assert!(json.contains("\"cpu_pct\":13.7"));
        assert!(json.contains("\"ip\":\"192.168.1.20\""));
        // Defensive: make sure we don't accidentally use the alternative names.
        assert!(!json.contains("memory_percent"));
        assert!(!json.contains("ip_address"));
    }

    #[test]
    fn payload_matches_consumer_contract() {
        // Defense: parse back using a struct resembling the consumer side.
        // If a field is renamed on either side, this test fails first instead
        // of becoming a runtime mystery.
        #[derive(serde::Deserialize)]
        struct ConsumerContract {
            memory_pct: f64,
            cpu_pct: f64,
            ip: String,
        }
        let payload = ResourcePayload {
            memory_pct: 50.0,
            cpu_pct: 25.0,
            ip: "10.0.0.5",
        };
        let bytes = serde_json::to_vec(&payload).unwrap();
        let parsed: ConsumerContract = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.memory_pct, 50.0);
        assert_eq!(parsed.cpu_pct, 25.0);
        assert_eq!(parsed.ip, "10.0.0.5");
    }

    #[test]
    fn memory_pct_handles_zero_total() {
        // Edge case: system info unavailable, total memory reported as 0.
        // Must not panic (divide-by-zero) — returns 0.0.
        assert_eq!(compute_memory_pct(0, 0), 0.0);
        assert_eq!(compute_memory_pct(1000, 0), 0.0);
    }

    #[test]
    fn memory_pct_basic_computation() {
        assert_eq!(compute_memory_pct(500, 1000), 50.0);
        assert_eq!(compute_memory_pct(0, 1000), 0.0);
        assert_eq!(compute_memory_pct(1000, 1000), 100.0);
    }
}
