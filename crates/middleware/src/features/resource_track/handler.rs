//! Pure handler: parse a resource message and record it to storage.
//!
//! Same shape as `heartbeat_track::handler`. The JSON field names match the
//! Rust struct (`memory_pct`, `cpu_pct`, `ip`), as published by edge-client.

use serde::Deserialize;
use tracing::{debug, warn};

use crate::shared::storage::{ResourceRow, StorageRepo};

/// Resource payload from edge-client. Field names match this struct
/// (snake_case, with a unit suffix where relevant).
///
/// Extra fields (for example a timestamp from the edge clock) are ignored by
/// serde. The middleware uses its own clock.
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

    // No range check: values are stored as-is and outliers are left to the
    // dashboard or alerting.
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
