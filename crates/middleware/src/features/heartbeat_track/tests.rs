//! Unit tests untuk heartbeat track. Pakai manual MockStorage yang record
//! heartbeat call ke Vec di Mutex. Stub method lain ke no-op karena feature
//! ini cuma pakai `record_heartbeat`.

use super::*;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Mutex;

use crate::shared::mqtt_client::MqttPublisher;
use crate::shared::storage::{
    ActuatorRow, HeartbeatRow, PlcStatusRow, ResourceRow, SetpointRow, StorageRepo, TelemetryRow,
};

/// No-op publisher — heartbeat tests fokus ke storage record, bukan publish.
#[derive(Default)]
struct NoopPublisher;

#[async_trait]
impl MqttPublisher for NoopPublisher {
    async fn publish(&self, _topic: &str, _payload: &[u8], _qos: i32, _retain: bool) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockStorage
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockStorage {
    heartbeat: Mutex<Vec<HeartbeatRow>>,
}

#[async_trait]
impl StorageRepo for MockStorage {
    fn record_telemetry(&self, _row: TelemetryRow) {}
    fn record_setpoint_history(&self, _row: SetpointRow) {}
    fn record_setpoint_current(&self, _row: SetpointRow) {}
    fn record_actuator_history(&self, _row: ActuatorRow) {}
    fn record_actuator_current(&self, _row: ActuatorRow) {}
    fn record_heartbeat(&self, row: HeartbeatRow) {
        self.heartbeat.lock().unwrap().push(row);
    }
    fn record_plc_status(&self, _row: PlcStatusRow) {
        // Not exercised by heartbeat_track tests.
    }
    fn record_resource(&self, _row: ResourceRow) {
        // Not exercised by heartbeat_track tests.
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

fn fixture() -> (HeartbeatTrackService, Arc<MockStorage>) {
    let storage: Arc<MockStorage> = Arc::new(MockStorage::default());
    let service = HeartbeatTrackService::new(
        "acme/site",
        &["area1/machine_a".into(), "area1/machine_b".into()],
        storage.clone() as Arc<dyn StorageRepo>,
        Arc::new(NoopPublisher) as Arc<dyn MqttPublisher>,
        1,
    );
    (service, storage)
}

// ---------------------------------------------------------------------------
// owns_topic
// ---------------------------------------------------------------------------

#[test]
fn owns_topic_matches_known_machine() {
    let (svc, _) = fixture();
    assert!(svc.owns_topic("acme/site/area1/machine_a/heartbeat"));
    assert!(svc.owns_topic("acme/site/area1/machine_b/heartbeat"));
}

#[test]
fn owns_topic_rejects_unknown_machine() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area2/machine_c/heartbeat"));
}

#[test]
fn owns_topic_rejects_non_heartbeat_suffix() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("acme/site/area1/machine_a/current/motor"));
    assert!(!svc.owns_topic("acme/site/area1/machine_a/resources"));
}

#[test]
fn owns_topic_rejects_foreign_base() {
    let (svc, _) = fixture();
    assert!(!svc.owns_topic("other/base/area1/machine_a/heartbeat"));
}

// ---------------------------------------------------------------------------
// handle: happy paths
// ---------------------------------------------------------------------------

#[test]
fn handle_connected_records_row() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/heartbeat",
        r#"{"status":"connected"}"#,
        1_700_000_000_000,
    );
    let rows = storage.heartbeat.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].machine, "area1/machine_a");
    assert_eq!(rows[0].status, "connected");
    assert_eq!(rows[0].ts_ms, 1_700_000_000_000);
}

#[test]
fn handle_disconnected_records_row() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_b/heartbeat",
        r#"{"status":"disconnected"}"#,
        1_700_000_001_000,
    );
    let rows = storage.heartbeat.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].machine, "area1/machine_b");
    assert_eq!(rows[0].status, "disconnected");
}

// ---------------------------------------------------------------------------
// handle: validation
// ---------------------------------------------------------------------------

#[test]
fn handle_rejects_invalid_json() {
    let (svc, storage) = fixture();
    svc.handle_at("acme/site/area1/machine_a/heartbeat", "not json", 1);
    svc.handle_at("acme/site/area1/machine_a/heartbeat", "{", 2);
    assert!(storage.heartbeat.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_unknown_status() {
    let (svc, storage) = fixture();
    svc.handle_at(
        "acme/site/area1/machine_a/heartbeat",
        r#"{"status":"frozen"}"#,
        1,
    );
    svc.handle_at("acme/site/area1/machine_a/heartbeat", r#"{"status":""}"#, 2);
    assert!(storage.heartbeat.lock().unwrap().is_empty());
}

#[test]
fn handle_rejects_missing_status_field() {
    let (svc, storage) = fixture();
    svc.handle_at("acme/site/area1/machine_a/heartbeat", r#"{}"#, 1);
    svc.handle_at(
        "acme/site/area1/machine_a/heartbeat",
        r#"{"other":"connected"}"#,
        2,
    );
    assert!(storage.heartbeat.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// handle: defense-in-depth (caller violates contract by calling for foreign topic)
// ---------------------------------------------------------------------------

#[test]
fn handle_silently_drops_foreign_topic() {
    let (svc, storage) = fixture();
    // Bukan topic milik service: tidak match prefix
    svc.handle_at(
        "other/base/area1/machine_a/heartbeat",
        r#"{"status":"connected"}"#,
        1,
    );
    // Match prefix tapi machine tidak dikenal
    svc.handle_at(
        "acme/site/area2/machine_c/heartbeat",
        r#"{"status":"connected"}"#,
        2,
    );
    assert!(storage.heartbeat.lock().unwrap().is_empty());
}
