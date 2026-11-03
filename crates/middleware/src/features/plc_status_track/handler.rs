//! Pure handler: terima plc/status message → parse JSON → record ke storage.
//!
//! Mirror pola `heartbeat_track::handler`. Payload format identik dengan
//! heartbeat (`{"status": "connected"|"disconnected"}`) — lihat
//! `edge-client/src/telemetry.rs::PAYLOAD_PLC_CONNECTED`. Tabel berbeda
//! supaya semantik (edge↔MQTT vs edge↔PLC) bisa dibedakan downstream.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{PlcStatusRow, StorageRepo};

/// Payload PLC status dari edge-client. Field `status` saja yang dipakai —
/// kalau edge tambah field lain (mis. `error_code`, `last_address`) di future,
/// serde akan ignore by default sampai schema di-extend.
#[derive(Debug, Deserialize)]
struct PlcStatusPayload {
    status: String,
}

/// Proses satu plc/status message. Caller (service) sudah validasi bahwa
/// `machine` ada di daftar yang diizinkan, dan sudah ekstrak dari topic.
pub fn handle(storage: &dyn StorageRepo, machine: &str, payload: &str, ts_ms: i64) {
    let parsed: PlcStatusPayload = match serde_json::from_str(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(%machine, %payload, "plc_status JSON parse failed: {e:#}");
            return;
        }
    };

    // Sanitasi: status harus salah satu nilai yang diakui. Yang tidak diakui
    // di-warn tapi tetap drop supaya tidak corrupt data history.
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
