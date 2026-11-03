//! Pure handler: terima resource message → parse JSON → record ke storage.
//!
//! Mirror pola `heartbeat_track::handler`. Field name di JSON match struct
//! Rust langsung (`memory_pct`, `cpu_pct`, `ip`). Edge-client publish pakai
//! shape ini.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{ResourceRow, StorageRepo};

/// Payload resource dari edge-client. Field name match struct internal
/// (snake_case, unit-suffixed di mana relevan).
///
/// Catatan: kalau payload punya field tambahan (mis. `published_ts_ms` dari
/// edge clock), serde default = ignored. Server pakai clock-nya sendiri.
#[derive(Debug, Deserialize)]
struct ResourcePayload {
    memory_pct: f64,
    cpu_pct: f64,
    ip: String,
}

pub fn handle(storage: &dyn StorageRepo, machine: &str, payload: &str, ts_ms: i64) {
    let parsed: ResourcePayload = match serde_json::from_str(payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(%machine, %payload, "resource JSON parse failed: {e:#}");
            return;
        }
    };

    // No range validation by design — biarkan dashboard/alerting yang detect
    // outlier. Tugas middleware: persist as-is. Cuma log debug.
    debug!(
        %machine,
        memory_pct = parsed.memory_pct,
        cpu_pct = parsed.cpu_pct,
        ip = %parsed.ip,
        "resource received"
    );

    storage.record_resource(ResourceRow {
        machine: machine.to_string(),
        ts_ms,
        memory_pct: parsed.memory_pct,
        cpu_pct: parsed.cpu_pct,
        ip: parsed.ip,
    });
}
