//! PLC status track feature: subscribe ke `{base}/{machine}/plc/status` per
//! mesin, persist status koneksi Modbus edge↔PLC ke storage.
//!
//! Edge-client publish (lihat `edge-client/src/telemetry.rs`):
//!   - `{"status": "connected"}` — saat Modbus reader bisa baca PLC.
//!   - `{"status": "disconnected"}` — saat connect gagal atau read error
//!     mid-poll (retained, supaya subscriber baru langsung dapat last state).
//!
//! Beda dari `heartbeat_track`: heartbeat = edge↔MQTT broker, plc_status =
//! edge↔Modbus PLC. State paling kritikal: heartbeat connected + plc_status
//! disconnected = "edge running but plant blind". Disimpan terpisah supaya
//! dashboard bisa bedakan.
//!
//! Middleware route topic ini via `owns_topic` di routing main.rs, sebelum
//! fall-through ke `telemetry_ingest` (yang akan reject karena tidak ada di
//! TopicIndex).

mod handler;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::shared::storage::StorageRepo;
use crate::shared::time::now_ms;

const PLC_STATUS_SUFFIX: &str = "/plc/status";

/// Service handle. Cheap to clone (cuma Arc).
#[derive(Clone)]
pub struct PlcStatusTrackService {
    /// Prefix `{base_topic}/` — pre-formatted untuk fast prefix check.
    base_prefix: Arc<String>,
    /// Set mesin yang valid (dari `MQTT_MACHINES`). Topic dari mesin di luar
    /// set ini di-reject di `owns_topic`.
    machines: Arc<HashSet<String>>,
    storage: Arc<dyn StorageRepo>,
}

impl PlcStatusTrackService {
    pub fn new(base_topic: &str, machines: &[String], storage: Arc<dyn StorageRepo>) -> Self {
        Self {
            base_prefix: Arc::new(format!("{}/", base_topic)),
            machines: Arc::new(machines.iter().cloned().collect()),
            storage,
        }
    }

    /// True kalau topic ini milik service. Shape valid:
    /// `{base}/{machine}/plc/status` dengan machine ∈ MQTT_MACHINES.
    pub fn owns_topic(&self, topic: &str) -> bool {
        self.extract_machine(topic).is_some()
    }

    /// Ekstrak nama machine dari topic. None kalau topic tidak match shape
    /// atau machine tidak dikenal.
    fn extract_machine<'a>(&self, topic: &'a str) -> Option<&'a str> {
        let middle = topic
            .strip_prefix(self.base_prefix.as_str())?
            .strip_suffix(PLC_STATUS_SUFFIX)?;
        if self.machines.contains(middle) {
            Some(middle)
        } else {
            None
        }
    }

    /// Proses satu pesan masuk. Caller (main loop) sudah panggil `owns_topic`
    /// untuk filter; di sini kita re-extract machine (tidak ekspensif).
    pub fn handle(&self, topic: &str, payload: &str) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, now_ms());
        }
        // Else: caller tidak seharusnya panggil ini untuk topic foreign;
        // silently drop kalau kejadian (defense in depth).
    }

    /// Variant test-friendly: inject timestamp.
    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, ts_ms);
        }
    }
}
