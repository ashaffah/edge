//! Unit tests untuk resource track. MockStorage record `record_resource`,
//! method lain no-op (sama strategi seperti `heartbeat_track::tests`).

use super::*;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Mutex;

use crate::shared::storage::{
    ActuatorRow, HeartbeatRow, PlcStatusRow, ResourceRow, SetpointRow, StorageRepo, TelemetryRow,
};

// ---------------------------------------------------------------------------
// MockStorage
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockStorage {
    resource: Mutex<Vec<ResourceRow>>,
}

#[async_trait]
impl StorageRepo for MockStorage {
    fn record_telemetry(&self, _row: TelemetryRow) {}
    fn record_setpoint_history(&self, _row: SetpointRow) {}
    fn record_setpoint_current(&self, _row: SetpointRow) {}
    fn record_actuator_history(&self, _row: ActuatorRow) {}
    fn record_actuator_current(&self, _row: ActuatorRow) {}
    fn record_heartbeat(&self, _row: HeartbeatRow) {}
    fn record_plc_status(&self, _row: PlcStatusRow) {}
    fn record_resource(&self, row: ResourceRow) {
        self.resource.lock().unwrap().push(row);
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

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn fixture() -> (ResourceTrackService, Arc<MockStorage>) {
    let storage: Arc<MockStorage> = Arc::new(MockStorage::default());
    let service = ResourceTrackService::new(
        "acme/site",
        &["area1/machine_a".into(), "area1/machine_b".into()],
        storage.clone() as Arc<dyn StorageRepo>,
    );
    (service, storage)
}

// ---------------------------------------------------------------------------
// owns_topic
// ---------------------------------------------------------------------------

#[test]
fn owns_topic_matches_known_machine() {
    let (svc, _) = fixture();
    assert!(svc.owns_topic("acme/site/area1/machine_a/resources"));
    assert!(svc.owns_topic("acme/site/area1/machine_b/resources"));
}

#[test]
fn owns_topic_rejects_unknown_machine() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area2/machine_c/resources"));
}

#[test]
fn owns_topic_rejects_non_resources_suffix() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area1/machine_a/heartbeat"));
    assert!(!svc.owns_topic("acme/site/area1/machine_a/current/motor"));
}

#[test]
fn owns_topic_rejects_foreign_base() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("other/base/area1/machine_a/resources"));
}

// ---------------------------------------------------------------------------
// handle: happy paths
// ---------------------------------------------------------------------------

#[test]
fn handle_full_payload_records_row() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 45.2, "cpu_pct": 12.5, "ip": "10.0.0.5"}"#,
        1_700_000_000_000,
    );
    let rows = storage.resource.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].machine, "area1/machine_a");
    assert_eq!(rows[0].ts_ms, 1_700_000_000_000);
    assert!((rows[0].memory_pct - 45.2).abs() < 1e-9);
    assert!((rows[0].cpu_pct - 12.5).abs() < 1e-9);
    assert_eq!(rows[0].ip, "10.0.0.5");
}

#[test]
fn handle_ignores_extra_fields() {
    // Field tambahan di payload (mis. published_ts_ms dari edge clock) di-ignore.
    // Server pakai clock sendiri.
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_b/resources",
        r#"{"memory_pct": 80.0, "cpu_pct": 90.0, "ip": "10.0.0.6", "published_ts_ms": 1500000000000}"#,
        2_000_000_000_000,
    );
    let rows = storage.resource.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].ts_ms, 2_000_000_000_000,
        "server clock, not payload"
    );
}

#[test]
fn handle_accepts_empty_ip() {
    // Network broken di edge → IP empty. Tetap di-record (design decision).
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 50.0, "cpu_pct": 20.0, "ip": ""}"#,
        1,
    );
    let rows = storage.resource.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ip, "");
}

#[test]
fn handle_accepts_out_of_range_pct() {
    // No range validation by design — dashboard yang detect anomaly.
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 150.0, "cpu_pct": -5.0, "ip": "10.0.0.5"}"#,
        1,
    );
    let rows = storage.resource.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert!((rows[0].memory_pct - 150.0).abs() < 1e-9);
    assert!((rows[0].cpu_pct - (-5.0)).abs() < 1e-9);
}

// ---------------------------------------------------------------------------
// handle: validation
// ---------------------------------------------------------------------------

#[test]
fn handle_rejects_invalid_json() {
    let (svc, storage) = fixture();
    svc.handle_at("acme/site/area1/machine_a/resources", "not json", 1);
    svc.handle_at("acme/site/area1/machine_a/resources", "{}", 2);
    assert!(storage.resource.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_missing_field() {
    let (svc, storage) = fixture();
    // Missing memory_pct
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"cpu_pct": 20.0, "ip": "10.0.0.5"}"#,
        1,
    );
    // Missing cpu_pct
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 50.0, "ip": "10.0.0.5"}"#,
        2,
    );
    // Missing ip
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 50.0, "cpu_pct": 20.0}"#,
        3,
    );
    assert!(storage.resource.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_wrong_type() {
    // cpu_pct sebagai string → parse fail
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_pct": 50.0, "cpu_pct": "twenty", "ip": "10.0.0.5"}"#,
        1,
    );
    assert!(storage.resource.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_legacy_python_field_names() {
    // Defense: payload pakai nama Python lama (memory_usage/cpu_usage/ip_address)
    // sekarang harus REJECTED. Kalau ini suatu hari pass, ada regression ke compat mode.
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/resources",
        r#"{"memory_usage": 50.0, "cpu_usage": 20.0, "ip_address": "10.0.0.5"}"#,
        1,
    );
    assert!(
        storage.resource.lock().unwrap().is_empty(),
        "legacy Python field names should not be accepted"
    );
}

// ---------------------------------------------------------------------------
// handle: defense in depth
// ---------------------------------------------------------------------------

#[test]
fn handle_silently_drops_foreign_topic() {
    let (svc, storage) = fixture();
    let payload = r#"{"memory_pct": 50.0, "cpu_pct": 20.0, "ip": "10.0.0.5"}"#;
    svc.handle_at("other/base/area1/machine_a/resources", payload, 1);
    svc.handle_at("acme/site/area2/machine_c/resources", payload, 2);
    assert!(storage.resource.lock().unwrap().is_empty());
}
