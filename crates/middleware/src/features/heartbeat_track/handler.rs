//! Pure handler: terima heartbeat message → parse JSON → record ke storage.
//!
//! Tidak nyentuh MQTT/Scylla langsung; semua via trait. Mirror pola
//! `telemetry_ingest::handler` supaya konsisten.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{HeartbeatRow, StorageRepo};

/// Payload heartbeat dari edge-client:
/// `{"status": "connected"}` saat alive, `{"status": "disconnected"}` saat
/// LWT (last-will-testament) di broker.
#[derive(Debug, Deserialize)]
struct HeartbeatPayload {
    status: String,
}

/// Proses satu heartbeat message. Caller (service) sudah validasi bahwa
/// `machine` ada di daftar yang diizinkan, dan sudah ekstrak dari topic.
///
/// Return `Some(true)` kalau status `connected`, `Some(false)` kalau
/// `disconnected`, `None` kalau payload invalid / status tidak dikenal
/// (tidak di-record). Caller pakai nilai ini untuk update liveness tracker.
pub fn handle(storage: &dyn StorageRepo, machine: &str, payload: &str, ts_ms: i64) -> Option<bool> {
    let parsed: HeartbeatPayload = match serde_json::from_str(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(%machine, %payload, "heartbeat JSON parse failed: {e:#}");
            return None;
        }
    };

    // Sanitasi: status harus salah satu nilai yang diakui. Yang tidak diakui
    // di-warn tapi tetap drop supaya tidak corrupt data history.
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
