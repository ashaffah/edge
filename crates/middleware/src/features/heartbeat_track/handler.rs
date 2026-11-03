//! Pure handler: parse a heartbeat message and record it to storage.
//!
//! No direct MQTT or Scylla access, only traits. Same shape as
//! `telemetry_ingest::handler`.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{HeartbeatRow, StorageRepo};

/// Heartbeat payload from edge-client: `{"status": "connected"}` while alive,
/// `{"status": "disconnected"}` as the LWT (last will and testament) sent by the broker.
#[derive(Debug, Deserialize)]
struct HeartbeatPayload {
    status: String,
}

/// Process one heartbeat message. The caller (service) already extracted
/// `machine` from the topic and checked that it is known.
///
/// Returns `Some(true)` for `connected`, `Some(false)` for `disconnected`, and
/// `None` for an invalid payload or unknown status (not recorded). The caller
/// uses the value to update the liveness tracker.
pub fn handle(storage: &dyn StorageRepo, machine: &str, payload: &str, ts_ms: i64) -> Option<bool> {
    let parsed: HeartbeatPayload = match serde_json::from_str(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(%machine, %payload, "heartbeat JSON parse failed: {e:#}");
            return None;
        }
    };

    // Only known status values are stored. Anything else is logged and
    // dropped so it does not end up in the history.
    let connected = match parsed.status.as_str() {
        "connected" => true,
        "disconnected" => false,
        other => {
            warn!(%machine, status = %other, "unknown heartbeat status, ignoring");
            return None;
        }
    };

    debug!(%machine, status = %parsed.status, "heartbeat received");
    storage.record_heartbeat(HeartbeatRow {
        machine: machine.to_string(),
        status: parsed.status,
        ts_ms,
    });
    Some(connected)
}
