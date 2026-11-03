//! Tests untuk command dispatch. Pakai dua manual mock:
//!   - MockStorage: configurable current values + umur (untuk dedup), record record_*.
//!   - MockPublisher: record (topic, payload, qos).

use super::*;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::shared::mqtt_client::MqttPublisher;
use crate::shared::storage::{
    ActuatorRow, HeartbeatRow, PlcStatusRow, ResourceRow, SetpointRow, StorageRepo, TelemetryRow,
};
use edge_shared::{MiddlewareMapping, TopicIndex};

// ---------------------------------------------------------------------------
// MockStorage with configurable current values
// ---------------------------------------------------------------------------

type MachineKey = (String, String);

#[derive(Default)]
struct MockStorage {
    setpoint_current: Mutex<HashMap<MachineKey, (f64, i64)>>,
    actuator_current: Mutex<HashMap<MachineKey, (bool, i64)>>,
    setpoint_history: Mutex<Vec<SetpointRow>>,
    actuator_history: Mutex<Vec<ActuatorRow>>,
}

impl MockStorage {
    /// Simulasi readback: nilai current dengan umur `age_ms`.
    fn set_setpoint(&self, machine: &str, key: &str, value: f64, age_ms: i64) {
        self.setpoint_current.lock().unwrap().insert(
            (machine.to_string(), key.to_string()),
            (value, now_ms() - age_ms),
        );
    }
    fn set_actuator(&self, machine: &str, key: &str, state: bool, age_ms: i64) {
        self.actuator_current.lock().unwrap().insert(
            (machine.to_string(), key.to_string()),
            (state, now_ms() - age_ms),
        );
    }
    fn actuator_state(&self, machine: &str, key: &str) -> Option<bool> {
        self.actuator_current
            .lock()
            .unwrap()
            .get(&(machine.to_string(), key.to_string()))
            .map(|(s, _)| *s)
    }
}

#[async_trait]
impl StorageRepo for MockStorage {
    fn record_telemetry(&self, _row: TelemetryRow) {
        // Tidak relevan untuk command dispatch.
    }
    fn record_setpoint_history(&self, row: SetpointRow) {
        self.setpoint_history.lock().unwrap().push(row);
    }
    fn record_setpoint_current(&self, row: SetpointRow) {
        self.setpoint_current
            .lock()
            .unwrap()
            .insert((row.machine, row.key), (row.value, row.ts_ms));
    }
    fn record_actuator_history(&self, row: ActuatorRow) {
        self.actuator_history.lock().unwrap().push(row);
    }
    fn record_actuator_current(&self, row: ActuatorRow) {
        self.actuator_current
            .lock()
            .unwrap()
            .insert((row.machine, row.key), (row.state, row.ts_ms));
    }
    fn record_heartbeat(&self, _row: HeartbeatRow) {
        // Tidak relevan untuk command dispatch.
    }
    fn record_plc_status(&self, _row: PlcStatusRow) {
        // Tidak relevan untuk command dispatch.
    }
    fn record_resource(&self, _row: ResourceRow) {
        // Tidak relevan untuk command dispatch.
    }
    async fn read_setpoint_current(&self, machine: &str, key: &str) -> Result<Option<(f64, i64)>> {
        Ok(self
            .setpoint_current
            .lock()
            .unwrap()
            .get(&(machine.to_string(), key.to_string()))
            .copied())
    }
    async fn read_actuator_current(&self, machine: &str, key: &str) -> Result<Option<(bool, i64)>> {
        Ok(self
            .actuator_current
            .lock()
            .unwrap()
            .get(&(machine.to_string(), key.to_string()))
            .copied())
    }
}

// ---------------------------------------------------------------------------
// MockPublisher — record semua publish.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockPublisher {
    calls: Mutex<Vec<(String, Vec<u8>, i32)>>,
}

impl MockPublisher {
    fn calls(&self) -> Vec<(String, Vec<u8>, i32)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl MqttPublisher for MockPublisher {
    async fn publish(&self, topic: &str, payload: &[u8], qos: i32, _retain: bool) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push((topic.to_string(), payload.to_vec(), qos));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn fixture() -> (CommandDispatchService, Arc<MockStorage>, Arc<MockPublisher>) {
    let mapping_json = r#"{
        "base_topic": "acme/site",
        "devices": [
            {
                "location": "area1",
                "name": "machine_a",
                "monitoring": [
                    { "key": "current_motor", "label": "Motor Current", "topic": "current/motor",
                      "type": "float", "unit": "A" }
                ],
                "set": [
                    { "key": "set_temp_main_pot", "label": "Set Temp", "topic": "set/temp/main",
                      "type": "float", "unit": "C" },
                    { "key": "set_time", "label": "Set Time", "topic": "set/time",
                      "type": "integer", "unit": "s" }
                ],
                "control": [
                    { "key": "control_lamp", "label": "Lamp On/Off", "topic": "control/lamp",
                      "type": "boolean" }
                ]
            }
        ]
    }"#;
    let mapping: MiddlewareMapping = serde_json::from_str(mapping_json).unwrap();
    let index = TopicIndex::build("acme/site", &mapping);
    let storage: Arc<MockStorage> = Arc::new(MockStorage::default());
    let publisher: Arc<MockPublisher> = Arc::new(MockPublisher::default());
    let service = CommandDispatchService::new(
        "acme/site".into(),
        &index,
        storage.clone() as Arc<dyn StorageRepo>,
        publisher.clone() as Arc<dyn MqttPublisher>,
        1,
    );
    (service, storage, publisher)
}

/// Umur state yang sudah lewat batas dedup.
const STALE_MS: i64 = STATE_MAX_AGE_MS + 1_000;

// ---------------------------------------------------------------------------
// Cmd index + topic ownership
// ---------------------------------------------------------------------------

#[test]
fn subscribe_pattern_is_global_cmd_wildcard() {
    let (service, _, _) = fixture();
    assert_eq!(service.subscribe_pattern(), "acme/site/cmd/#");
}

#[test]
fn owns_topic_matches_cmd_prefix() {
    let (service, _, _) = fixture();
    assert!(service.owns_topic("acme/site/cmd/area1/machine_a/set/temp/main"));
    assert!(service.owns_topic("acme/site/cmd/anything"));
    assert!(!service.owns_topic("acme/site/area1/machine_a/current/motor"));
}

// ---------------------------------------------------------------------------
// Set: happy path, dedup, validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_dispatches_publishes_to_plc_and_records_history() {
    // Happy path: nilai baru → publish ke edge PLC topic + catat history.
    let (service, storage, publisher) = fixture();
    let cmd = br#"{"value": 80.0, "request_id": "req-1"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/set/temp/main", cmd)
        .await
        .expect("set dispatch harus sukses");

    let calls = publisher.calls();
    // Publish 1: ke PLC topic edge-client. Publish 2: ack.
    assert_eq!(calls.len(), 2, "harus ada 2 publish: PLC + ack");
    assert_eq!(
        calls[0].0, "acme/site/area1/machine_a/set/temp/main",
        "publish pertama harus ke edge PLC topic"
    );
    assert_eq!(
        calls[1].0, "acme/site/ack/req-1",
        "publish kedua harus ke ack topic"
    );
    let history = storage.setpoint_history.lock().unwrap();
    assert_eq!(history.len(), 1, "harus tulis 1 row ke setpoint_history");
    assert_eq!(history[0].value, 80.0);
    assert!(
        storage.setpoint_current.lock().unwrap().is_empty(),
        "command tidak boleh menulis setpoint_current (hanya readback)"
    );
}

#[tokio::test]
async fn set_dedup_skips_when_fresh_state_same_value() {
    // Readback segar dengan nilai sama → skip publish PLC + skip history.
    let (service, storage, publisher) = fixture();
    storage.set_setpoint("area1/machine_a", "set_temp_main_pot", 70.0, 0);

    let cmd = br#"{"value": 70.0, "request_id": "req-dup"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/set/temp/main", cmd)
        .await
        .expect("dedup skip harus return Ok");

    let calls = publisher.calls();
    // Hanya ack "skipped" — tidak ada publish ke PLC.
    assert_eq!(calls.len(), 1, "hanya ack yang di-publish saat dedup");
    assert_eq!(calls[0].0, "acme/site/ack/req-dup");
    assert_eq!(
        storage.setpoint_history.lock().unwrap().len(),
        0,
        "tidak boleh tulis history saat dedup"
    );
}

#[tokio::test]
async fn set_forwards_when_state_stale_even_if_same_value() {
    // Readback basi (edge mati / row lama) → state tidak diketahui, forward.
    let (service, storage, publisher) = fixture();
    storage.set_setpoint("area1/machine_a", "set_temp_main_pot", 70.0, STALE_MS);

    let cmd = br#"{"value": 70.0, "request_id": "req-stale"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/set/temp/main", cmd)
        .await
        .unwrap();

    let calls = publisher.calls();
    assert_eq!(calls.len(), 2, "PLC + ack");
    assert_eq!(calls[0].0, "acme/site/area1/machine_a/set/temp/main");
}

#[tokio::test]
async fn set_integer_rejects_fractional_value() {
    // Integer-typed param + fractional payload → Err (validasi di value_as_f64).
    let (service, _, publisher) = fixture();
    let cmd = br#"{"value": 3.5, "request_id": "req-frac"}"#;
    let res = service
        .dispatch_sync("acme/site/cmd/area1/machine_a/set/time", cmd)
        .await;
    assert!(
        res.is_err(),
        "fractional value untuk integer param harus Err"
    );
    assert_eq!(publisher.calls().len(), 0, "tidak boleh publish saat error");
}

// ---------------------------------------------------------------------------
// Control: happy path, dedup, validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn control_publishes_when_state_changes() {
    let (service, storage, publisher) = fixture();
    storage.set_actuator("area1/machine_a", "control_lamp", false, 0);

    let cmd = br#"{"value": true, "request_id": "req-ctl"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/control/lamp", cmd)
        .await
        .unwrap();

    let calls = publisher.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, b"1");
    let history = storage.actuator_history.lock().unwrap();
    assert_eq!(history.len(), 1);
    assert!(history[0].state);
    assert_eq!(
        storage.actuator_state("area1/machine_a", "control_lamp"),
        Some(false),
        "command tidak boleh menimpa actuator_current (hanya readback)"
    );
}

#[tokio::test]
async fn control_skips_when_fresh_state_unchanged() {
    // Mis. lampu sudah dinyalakan dari HMI: readback true → command true di-skip.
    let (service, storage, publisher) = fixture();
    storage.set_actuator("area1/machine_a", "control_lamp", true, 0);

    let cmd = br#"{"value": 1, "request_id": "req-noop"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/control/lamp", cmd)
        .await
        .unwrap();

    let calls = publisher.calls();
    // Hanya ack "skipped".
    assert_eq!(calls.len(), 1);
    let ack: serde_json::Value = serde_json::from_slice(&calls[0].1).unwrap();
    assert_eq!(ack["status"], "skipped");
    assert_eq!(storage.actuator_history.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn control_forwards_when_state_stale() {
    let (service, storage, publisher) = fixture();
    storage.set_actuator("area1/machine_a", "control_lamp", true, STALE_MS);

    let cmd = br#"{"value": true, "request_id": "req-stale"}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/control/lamp", cmd)
        .await
        .unwrap();

    let calls = publisher.calls();
    assert_eq!(calls.len(), 2, "state basi: PLC + ack");
    assert_eq!(calls[0].1, b"1");
}

#[tokio::test]
async fn control_resend_after_gate_deny_is_forwarded_again() {
    // Command pertama di-forward tapi di-deny gate di edge: PLC tetap off,
    // readback tetap false. Kirim ulang nilai yang sama harus di-forward lagi,
    // bukan di-skip dedup.
    let (service, storage, publisher) = fixture();
    storage.set_actuator("area1/machine_a", "control_lamp", false, 0);

    let cmd = br#"{"value": true}"#;
    for _ in 0..2 {
        service
            .dispatch_sync("acme/site/cmd/area1/machine_a/control/lamp", cmd)
            .await
            .unwrap();
    }

    let plc_publishes = publisher
        .calls()
        .iter()
        .filter(|c| c.0 == "acme/site/area1/machine_a/control/lamp")
        .count();
    assert_eq!(plc_publishes, 2);
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_command_topic_errors() {
    let (service, _, publisher) = fixture();
    let res = service
        .dispatch_sync("acme/site/cmd/nowhere/foo/bar", br#"{"value": 1}"#)
        .await;
    assert!(res.is_err());
    assert_eq!(publisher.calls().len(), 0);
}

#[tokio::test]
async fn invalid_json_errors() {
    let (service, _, publisher) = fixture();
    let res = service
        .dispatch_sync("acme/site/cmd/area1/machine_a/set/temp/main", b"not json")
        .await;
    assert!(res.is_err());
    assert_eq!(publisher.calls().len(), 0);
}

#[tokio::test]
async fn no_request_id_no_ack() {
    let (service, _, publisher) = fixture();
    let cmd = br#"{"value": true}"#;
    service
        .dispatch_sync("acme/site/cmd/area1/machine_a/control/lamp", cmd)
        .await
        .unwrap();

    let calls = publisher.calls();
    // Cuma 1 publish (ke PLC), no ack.
    assert_eq!(calls.len(), 1);
}
