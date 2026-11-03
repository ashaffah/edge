//! Pure handler: terima MQTT message → parse → record ke storage + cache.
//!
//! Tidak nyentuh MQTT, Scylla, atau Valkey langsung; semua via trait.

use std::collections::HashMap;

use tracing::{debug, error, warn};

use crate::shared::cache::CacheRepo;
use crate::shared::storage::{ActuatorRow, SetpointRow, StorageRepo, TelemetryRow};
use edge_shared::{Category, DataType, TopicEntry, TopicIndex, TypedValue};

/// Proses satu telemetry message. Caller bertanggung jawab menyediakan
/// timestamp (biasanya `time::now_ms()` di production, atau nilai tetap
/// di test).
///
/// Write-through pattern: setiap row ditulis ke `storage` (authoritative,
/// Scylla) DAN `cache` (Valkey/Redis mirror untuk fast read by future
/// features). Cache failure tidak fatal — sudah dihandle di dalam impl
/// `CacheRepo` (log warn, continue).
pub fn handle(
    index: &TopicIndex,
    state_index: &HashMap<String, TopicEntry>,
    storage: &dyn StorageRepo,
    cache: &dyn CacheRepo,
    topic: &str,
    payload: &str,
    ts_ms: i64,
) {
    if let Some(entry) = state_index.get(topic) {
        handle_state(entry, storage, cache, topic, payload, ts_ms);
        return;
    }

    let Some(entry) = index.lookup(topic) else {
        warn!(%topic, "received message for unknown topic, ignoring");
        return;
    };

    if !matches!(entry.category, Category::Monitoring) {
        // Command yang di-forward middleware sendiri atau di-publish langsung ke
        // topic edge. Belum tentu ditulis ke PLC (gate, Modbus error), jadi
        // bukan state. State datang dari topic readback.
        debug!(%topic, "command topic, not recorded as state");
        return;
    }

    let Some(value) = parse(entry.data_type, topic, payload) else {
        return;
    };
    let Some(v) = numeric_value(&value) else {
        warn!(%topic, "monitoring topic received non-numeric payload, skipping");
        return;
    };
    let row = TelemetryRow {
        machine: entry.machine.clone(),
        key: entry.key.clone(),
        ts_ms,
        value: v,
        unit: entry.unit.clone(),
    };
    cache.record_telemetry_current(&row);
    storage.record_telemetry(row);
}

/// Readback state dari edge-client: satu-satunya sumber `*_current`.
fn handle_state(
    entry: &TopicEntry,
    storage: &dyn StorageRepo,
    cache: &dyn CacheRepo,
    topic: &str,
    payload: &str,
    ts_ms: i64,
) {
    match entry.category {
        Category::Set => {
            let Some(value) = parse(entry.data_type, topic, payload) else {
                return;
            };
            let Some(v) = numeric_value(&value) else {
                warn!(%topic, "set state received non-numeric payload, skipping");
                return;
            };
            let row = SetpointRow {
                machine: entry.machine.clone(),
                key: entry.key.clone(),
                ts_ms,
                value: v,
                unit: entry.unit.clone(),
            };
            cache.record_setpoint_current(&row);
            storage.record_setpoint_current(row);
        }
        Category::Control => {
            // Readback control selalu boolean, apa pun `type` entry di mapping.
            let Some(TypedValue::Boolean(state)) = parse(DataType::Boolean, topic, payload) else {
                return;
            };
            let row = ActuatorRow {
                machine: entry.machine.clone(),
                key: entry.key.clone(),
                ts_ms,
                state,
            };
            cache.record_actuator_current(&row);
            storage.record_actuator_current(row);
        }
        Category::Monitoring => {}
    }
}

fn parse(data_type: DataType, topic: &str, payload: &str) -> Option<TypedValue> {
    match data_type.parse(payload) {
        Ok(v) => Some(v),
        Err(err) => {
            error!(
                %topic,
                payload = %payload,
                expected = ?data_type,
                "failed to parse payload: {err:#}"
            );
            None
        }
    }
}

fn numeric_value(v: &TypedValue) -> Option<f64> {
    match v {
        TypedValue::Float(f) => Some(*f),
        TypedValue::Integer(i) => Some(*i as f64),
        TypedValue::Boolean(_) | TypedValue::String(_) => None,
    }
}
