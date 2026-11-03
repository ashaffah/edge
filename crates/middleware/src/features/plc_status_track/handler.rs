//! Pure handler: parse a plc/status message and record it to storage.
//!
//! Same shape as `heartbeat_track::handler`, and the payload format is the
//! same as heartbeat (`{"status": "connected"|"disconnected"}`, see
//! `PAYLOAD_CONNECTED` in edge-client `plc_status.rs`). It goes to a separate
//! table so edge to MQTT and edge to PLC status stay distinct downstream.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{PlcStatusRow, StorageRepo};

/// PLC status payload from edge-client. Only `status` is used; serde ignores
/// any other field the edge may add later.
#[derive(Debug, Deserialize)]
struct PlcStatusPayload {
    status: String,
}

/// Process one plc/status message. The caller (service) already extracted
/// `machine` from the topic and checked that it is known.
pub fn handle(storage: &dyn StorageRepo, machine: &str, payload: &str, ts_ms: i64) {
    let parsed: PlcStatusPayload = match serde_json::from_str(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(%machine, %payload, "plc_status JSON parse failed: {e:#}");
            return;
        }
    };

    // Only known status values are stored. Anything else is logged and
    // dropped so it does not end up in the history.
    match parsed.status.as_str() {
        "connected" | "disconnected" => {}
        other => {
            warn!(%machine, status = %other, "unknown plc_status, ignoring");
            return;
        }
    }

    debug!(%machine, status = %parsed.status, "plc_status received");
    storage.record_plc_status(PlcStatusRow {
        machine: machine.to_string(),
        status: parsed.status,
        ts_ms,
    });
}
