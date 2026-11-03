//! Pure handler: parse an MQTT message and record it to storage and cache.
//!
//! No direct MQTT, Scylla or Valkey access, only traits.

use std::collections::HashMap;

use tracing::{debug, error, warn};

use crate::shared::cache::CacheRepo;
use crate::shared::storage::{ActuatorRow, SetpointRow, StorageRepo, TelemetryRow};
use edge_shared::{Category, DataType, TopicEntry, TopicIndex, TypedValue};

/// Process one telemetry message. The caller supplies the timestamp
/// (`time::now_ms()` in production, a fixed value in tests).
///
/// Write-through: every row goes to `storage` (Scylla, authoritative) and to
/// `cache` (Valkey/Redis mirror for fast reads). A cache failure is not fatal:
/// the `CacheRepo` implementation logs a warning and continues.
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
        // A command forwarded by the middleware or published straight to the
        // edge topic. It may never reach the PLC (gate, Modbus error), so it is
        // not state. State comes from the readback topics.
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

/// Readback state from edge-client, the only source of `*_current`.
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
            // Control readback is always boolean, whatever the entry's `type` in the mapping.
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
