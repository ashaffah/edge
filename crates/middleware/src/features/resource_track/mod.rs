//! Resource track feature: subscribe ke `{base}/{machine}/resources` per
//! mesin, persist snapshot CPU/memory/IP ke storage.
//!
//! Edge-client publish periodik (mis. tiap 10s) dengan payload JSON:
//! `{"memory_pct": 45.2, "cpu_pct": 12.5, "ip": "10.0.0.5"}`.
//! Field name langsung match struct Rust (lihat `handler.rs`).
//!
//! Topic shape identik pola dengan `heartbeat_track`, cuma suffix beda
//! (`/resources` vs `/heartbeat`). Tidak ada cache integration — belum ada
//! consumer baca current state.

mod handler;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::shared::storage::StorageRepo;
use crate::shared::time::now_ms;

const RESOURCES_SUFFIX: &str = "/resources";

/// Service handle. Cheap to clone (cuma Arc).
#[derive(Clone)]
pub struct ResourceTrackService {
    base_prefix: Arc<String>,
    machines: Arc<HashSet<String>>,
    storage: Arc<dyn StorageRepo>,
}

impl ResourceTrackService {
    pub fn new(base_topic: &str, machines: &[String], storage: Arc<dyn StorageRepo>) -> Self {
        Self {
            base_prefix: Arc::new(format!("{}/", base_topic)),
            machines: Arc::new(machines.iter().cloned().collect()),
            storage,
        }
    }

    /// True kalau topic ini milik service. Shape valid:
    /// `{base}/{machine}/resources` dengan machine ∈ MQTT_MACHINES.
    pub fn owns_topic(&self, topic: &str) -> bool {
        self.extract_machine(topic).is_some()
    }

    fn extract_machine<'a>(&self, topic: &'a str) -> Option<&'a str> {
        let middle = topic
            .strip_prefix(self.base_prefix.as_str())?
            .strip_suffix(RESOURCES_SUFFIX)?;
        if self.machines.contains(middle) {
            Some(middle)
        } else {
            None
        }
    }

    pub fn handle(&self, topic: &str, payload: &str) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, now_ms());
        }
    }

    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, ts_ms);
        }
    }
}
