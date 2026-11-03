//! Unit tests untuk telemetry ingest. Pakai manual MockStorage + MockCache
//! yang record method call ke Vec, di-protect Mutex supaya bisa di-baca dari
//! test thread.

use super::*;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Mutex;

use crate::shared::cache::{ActuatorCurrent, CacheRepo, SetpointCurrent, TelemetryCurrent};
use crate::shared::storage::{
    ActuatorRow, HeartbeatRow, PlcStatusRow, ResourceRow, SetpointRow, StorageRepo, TelemetryRow,
};
use edge_shared::{MiddlewareMapping, TopicIndex};

// ---------------------------------------------------------------------------
// MockStorage: record semua write, return None untuk read.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockStorage {
    telemetry: Mutex<Vec<TelemetryRow>>,
    setpoint_history: Mutex<Vec<SetpointRow>>,
    setpoint_current: Mutex<Vec<SetpointRow>>,
    actuator_history: Mutex<Vec<ActuatorRow>>,
    actuator_current: Mutex<Vec<ActuatorRow>>,
}

#[async_trait]
impl StorageRepo for MockStorage {
    fn record_telemetry(&self, row: TelemetryRow) {
        self.telemetry.lock().unwrap().push(row);
    }
    fn record_setpoint_history(&self, row: SetpointRow) {
        self.setpoint_history.lock().unwrap().push(row);
    }
    fn record_setpoint_current(&self, row: SetpointRow) {
        self.setpoint_current.lock().unwrap().push(row);
    }
    fn record_actuator_history(&self, row: ActuatorRow) {
        self.actuator_history.lock().unwrap().push(row);
    }
    fn record_actuator_current(&self, row: ActuatorRow) {
        self.actuator_current.lock().unwrap().push(row);
    }
    fn record_heartbeat(&self, _row: HeartbeatRow) {
        // Not exercised by telemetry_ingest tests.
    }
    fn record_plc_status(&self, _row: PlcStatusRow) {
        // Not exercised by telemetry_ingest tests.
    }
    fn record_resource(&self, _row: ResourceRow) {
        // Not exercised by telemetry_ingest tests.
    }
    async fn read_setpoint_current(
        &self,
        _machine: &str,
        _key: &str,
    ) -> Result<Option<(f64, i64)>> {
        Ok(None)
    }
    async fn read_actuator_current(
        &self,
        _machine: &str,
        _key: &str,
    ) -> Result<Option<(bool, i64)>> {
        Ok(None)
    }
}

impl MockStorage {
    fn state_and_history_empty(&self) -> bool {
        self.setpoint_history.lock().unwrap().is_empty()
            && self.setpoint_current.lock().unwrap().is_empty()
            && self.actuator_history.lock().unwrap().is_empty()
            && self.actuator_current.lock().unwrap().is_empty()
    }
}

// ---------------------------------------------------------------------------
// MockCache: record semua write, return None untuk read.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockCache {
    telemetry: Mutex<Vec<TelemetryRow>>,
    setpoint: Mutex<Vec<SetpointRow>>,
    actuator: Mutex<Vec<ActuatorRow>>,
}

#[async_trait]
impl CacheRepo for MockCache {
    fn record_telemetry_current(&self, row: &TelemetryRow) {
        self.telemetry.lock().unwrap().push(row.clone());
    }
    fn record_setpoint_current(&self, row: &SetpointRow) {
        self.setpoint.lock().unwrap().push(row.clone());
    }
    fn record_actuator_current(&self, row: &ActuatorRow) {
        self.actuator.lock().unwrap().push(row.clone());
    }
    async fn read_telemetry_current(&self, _m: &str, _k: &str) -> Result<Option<TelemetryCurrent>> {
        Ok(None)
    }
    async fn read_setpoint_current(&self, _m: &str, _k: &str) -> Result<Option<SetpointCurrent>> {
        Ok(None)
    }
    async fn read_actuator_current(&self, _m: &str, _k: &str) -> Result<Option<ActuatorCurrent>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn fixture() -> (TelemetryIngestService, Arc<MockStorage>, Arc<MockCache>) {
    let mapping_json = r#"{
        "base_topic": "acme/site",
        "poll_interval_ms": 1000,
        "devices": [{
            "location": "area1",
            "name": "machine_a",
            "monitoring": [
                { "key": "current_motor", "label": "Motor Current", "topic": "current/motor",
                  "type": "float", "unit": "A" },
                { "key": "run_time", "label": "Run Time", "topic": "run/time",
                  "type": "integer", "unit": "s" }
            ],
            "set": [
                { "key": "set_temp_main_pot", "label": "Set Temp", "topic": "set/temp/main",
                  "type": "float", "unit": "C" }
            ],
            "control": [
                { "key": "control_lamp", "label": "Lamp On/Off", "topic": "control/lamp",
                  "type": "boolean" }
            ]
        }]
    }"#;
    let mapping: MiddlewareMapping = serde_json::from_str(mapping_json).unwrap();
    let index = Arc::new(TopicIndex::build("acme/site", &mapping));
    let storage: Arc<MockStorage> = Arc::new(MockStorage::default());
    let cache: Arc<MockCache> = Arc::new(MockCache::default());
    let service = TelemetryIngestService::new(
        "acme/site",
        index,
        storage.clone() as Arc<dyn StorageRepo>,
        cache.clone() as Arc<dyn CacheRepo>,
    );
    (service, storage, cache)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn float_monitoring_routes_to_telemetry() {
    let (service, storage, cache) = fixture();
    service.handle_at(
        "acme/site/area1/machine_a/current/motor",
        "12.5",
        1_700_000_000_000,
    );

    let t = storage.telemetry.lock().unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].machine, "area1/machine_a");
    assert_eq!(t[0].key, "current_motor");
    assert!((t[0].value - 12.5).abs() < 1e-9);
    assert_eq!(t[0].ts_ms, 1_700_000_000_000);
    assert_eq!(t[0].unit.as_deref(), Some("A"));

    // Cache juga harus dapat write yang sama (write-through).
    let c = cache.telemetry.lock().unwrap();
    assert_eq!(c.len(), 1);
    assert!((c[0].value - 12.5).abs() < 1e-9);
}

#[test]
fn integer_monitoring_converted_to_f64() {
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/area1/machine_a/run/time", "120", 0);

    let t = storage.telemetry.lock().unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].key, "run_time");
    assert!((t[0].value - 120.0).abs() < 1e-9);

    assert_eq!(cache.telemetry.lock().unwrap().len(), 1);
}

#[test]
fn set_state_routes_to_setpoint_current() {
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/area1/machine_a/state/set/temp/main", "80.5", 7);

    assert_eq!(storage.telemetry.lock().unwrap().len(), 0);
    let s = storage.setpoint_current.lock().unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].key, "set_temp_main_pot");
    assert!((s[0].value - 80.5).abs() < 1e-9);
    assert_eq!(s[0].ts_ms, 7);
    assert!(
        storage.setpoint_history.lock().unwrap().is_empty(),
        "readback tidak masuk history"
    );

    let cs = cache.setpoint.lock().unwrap();
    assert_eq!(cs.len(), 1);
    assert!((cs[0].value - 80.5).abs() < 1e-9);
}

#[test]
fn control_state_routes_to_actuator_current() {
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/area1/machine_a/state/control/lamp", "true", 0);

    let a = storage.actuator_current.lock().unwrap();
    assert_eq!(a.len(), 1);
    assert!(a[0].state);
    assert!(storage.actuator_history.lock().unwrap().is_empty());

    let ca = cache.actuator.lock().unwrap();
    assert_eq!(ca.len(), 1);
    assert!(ca[0].state);
}

#[test]
fn command_topics_not_recorded_as_state() {
    // Forward middleware sendiri atau publish langsung ke topic edge: bisa di-deny
    // gate, jadi bukan state PLC.
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/area1/machine_a/control/lamp", "1", 0);
    service.handle_at("acme/site/area1/machine_a/set/temp/main", "80.0", 0);

    assert!(storage.state_and_history_empty());
    assert_eq!(storage.telemetry.lock().unwrap().len(), 0);
    assert_eq!(cache.setpoint.lock().unwrap().len(), 0);
    assert_eq!(cache.actuator.lock().unwrap().len(), 0);
}

#[test]
fn unknown_topic_silently_skipped() {
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/unknown/foo/bar", "1.0", 0);
    // State topic untuk param yang tidak ada di mapping juga tidak dikenal.
    service.handle_at("acme/site/area1/machine_a/state/control/unknown", "true", 0);

    assert_eq!(storage.telemetry.lock().unwrap().len(), 0);
    assert!(storage.state_and_history_empty());
    // Cache juga tidak boleh tertulis untuk topic tidak dikenal.
    assert_eq!(cache.telemetry.lock().unwrap().len(), 0);
    assert_eq!(cache.setpoint.lock().unwrap().len(), 0);
    assert_eq!(cache.actuator.lock().unwrap().len(), 0);
}

#[test]
fn invalid_payload_no_record() {
    let (service, storage, cache) = fixture();
    service.handle_at("acme/site/area1/machine_a/current/motor", "not_a_number", 0);
    service.handle_at("acme/site/area1/machine_a/state/set/temp/main", "abc", 0);
    assert_eq!(storage.telemetry.lock().unwrap().len(), 0);
    assert_eq!(cache.telemetry.lock().unwrap().len(), 0);
    assert!(storage.state_and_history_empty());
}

#[test]
fn control_state_with_non_boolean_skipped() {
    let (service, storage, cache) = fixture();
    // "2" bukan boolean valid.
    service.handle_at("acme/site/area1/machine_a/state/control/lamp", "2", 0);
    assert!(storage.actuator_current.lock().unwrap().is_empty());
    assert_eq!(cache.actuator.lock().unwrap().len(), 0);
}
