//! Unit tests untuk plc_status track. Pakai manual MockStorage yang record
//! plc_status call ke Vec di Mutex. Stub method lain ke no-op karena feature
//! ini cuma pakai `record_plc_status`.
//!
//! Sengaja mirror struktur `heartbeat_track::tests` supaya gampang
//! cross-reference: kalau pola routing/parsing di heartbeat berubah, plc_status
//! harus berubah sama.

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
    plc_status: Mutex<Vec<PlcStatusRow>>,
}

#[async_trait]
impl StorageRepo for MockStorage {
    fn record_telemetry(&self, _row: TelemetryRow) {}
    fn record_setpoint_history(&self, _row: SetpointRow) {}
    fn record_setpoint_current(&self, _row: SetpointRow) {}
    fn record_actuator_history(&self, _row: ActuatorRow) {}
    fn record_actuator_current(&self, _row: ActuatorRow) {}
    fn record_heartbeat(&self, _row: HeartbeatRow) {}
    fn record_plc_status(&self, row: PlcStatusRow) {
        self.plc_status.lock().unwrap().push(row);
    }
    fn record_resource(&self, _row: ResourceRow) {}
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

fn fixture() -> (PlcStatusTrackService, Arc<MockStorage>) {
    let storage: Arc<MockStorage> = Arc::new(MockStorage::default());
    let service = PlcStatusTrackService::new(
        "acme/site",
        &["area1/machine_a".into(), "area2/machine_c".into()],
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
    assert!(svc.owns_topic("acme/site/area1/machine_a/plc/status"));
    assert!(svc.owns_topic("acme/site/area2/machine_c/plc/status"));
}

#[test]
fn owns_topic_rejects_unknown_machine() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area2/chiller/plc/status"));
}

#[test]
fn owns_topic_rejects_non_plc_status_suffix() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area1/machine_a/heartbeat"));
    assert!(!svc.owns_topic("acme/site/area1/machine_a/resources"));
    // Suffix mirip tapi tidak persis: `/plc/state` bukan `/plc/status`.
    assert!(!svc.owns_topic("acme/site/area1/machine_a/plc/state"));
    // Prefix berbeda: bukan diakhiri dengan `/plc/status` setelah machine.
    assert!(!svc.owns_topic("acme/site/area1/machine_a/current/motor"));
}

#[test]
fn owns_topic_rejects_foreign_base() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("other/base/area1/machine_a/plc/status"));
}

// ---------------------------------------------------------------------------
// handle: happy paths
// ---------------------------------------------------------------------------

#[test]
fn handle_connected_records_row() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/plc/status",
        r#"{"status":"connected"}"#,
        1_700_000_000_000,
    );
    let rows = storage.plc_status.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].machine, "area1/machine_a");
    assert_eq!(rows[0].status, "connected");
    assert_eq!(rows[0].ts_ms, 1_700_000_000_000);
}

#[test]
fn handle_disconnected_records_row() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area2/machine_c/plc/status",
        r#"{"status":"disconnected"}"#,
        1_700_000_001_000,
    );
    let rows = storage.plc_status.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].machine, "area2/machine_c");
    assert_eq!(rows[0].status, "disconnected");
}

// ---------------------------------------------------------------------------
// handle: validation
// ---------------------------------------------------------------------------

#[test]
fn handle_rejects_invalid_json() {
    let (svc, storage) = fixture();
    svc.handle_at("acme/site/area1/machine_a/plc/status", "not json", 1);
    svc.handle_at("acme/site/area1/machine_a/plc/status", "{", 2);
    assert!(storage.plc_status.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_unknown_status() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/plc/status",
        r#"{"status":"degraded"}"#,
        1,
    );
    svc.handle_at(
        "acme/site/area1/machine_a/plc/status",
        r#"{"status":""}"#,
        2,
    );
    assert!(storage.plc_status.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_missing_status_field() {
    let (svc, storage) = fixture();
    svc.handle_at("acme/site/area1/machine_a/plc/status", r#"{}"#, 1);
    svc.handle_at(
        "acme/site/area1/machine_a/plc/status",
        r#"{"other":"connected"}"#,
        2,
    );
    assert!(storage.plc_status.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// handle: defense-in-depth (caller violates contract by calling for foreign topic)
// ---------------------------------------------------------------------------

#[test]
fn handle_silently_drops_foreign_topic() {
    let (svc, storage) = fixture();
    // Bukan topic milik service: tidak match prefix
    svc.handle_at(
        "other/base/area1/machine_a/plc/status",
        r#"{"status":"connected"}"#,
        1,
    );
    // Match prefix tapi machine tidak dikenal
    svc.handle_at(
        "acme/site/area2/chiller/plc/status",
        r#"{"status":"connected"}"#,
        2,
    );
    assert!(storage.plc_status.lock().unwrap().is_empty());
}
