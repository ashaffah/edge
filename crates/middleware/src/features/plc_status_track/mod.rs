//! PLC status track feature: subscribe to `{base}/{machine}/plc/status` for
//! each machine and persist the edge to PLC Modbus link status to storage.
//!
//! edge-client publishes (see `plc_status.rs` in edge-client):
//!   - `{"status": "connected"}` while the Modbus reader can read the PLC.
//!   - `{"status": "disconnected"}` when connecting or a read fails.
//!
//! Heartbeat covers edge to MQTT broker, plc_status covers edge to PLC. The
//! case that matters most is a connected heartbeat with a disconnected PLC:
//! the edge runs but reads nothing. They are stored separately so a dashboard
//! can tell them apart.
//!
//! main.rs routes these topics here through `owns_topic` before falling
//! through to `telemetry_ingest`, which would reject them because they are
//! not in the TopicIndex.

mod handler;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::shared::storage::StorageRepo;
use crate::shared::time::now_ms;

const PLC_STATUS_SUFFIX: &str = "/plc/status";

/// Service handle. Cheap to clone (only Arcs).
#[derive(Clone)]
pub struct PlcStatusTrackService {
    /// Pre-formatted `{base_topic}/` prefix for a fast prefix check.
    base_prefix: Arc<String>,
    /// Known machines (from the mapping). `owns_topic` rejects topics of any
    /// other machine.
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

    /// True if this service owns the topic. Valid shape:
    /// `{base}/{machine}/plc/status` where machine is in the mapping.
    pub fn owns_topic(&self, topic: &str) -> bool {
        self.extract_machine(topic).is_some()
    }

    /// Extract the machine from the topic. None if the shape does not match or
    /// the machine is unknown.
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

    /// Process one incoming message. The caller already filtered with
    /// `owns_topic`; the machine is extracted again here (cheap).
    pub fn handle(&self, topic: &str, payload: &str) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, now_ms());
        }
        // The caller should not pass a foreign topic. Drop it if it does.
    }

    /// Same as `handle`, with an injected timestamp for tests.
    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        if let Some(machine) = self.extract_machine(topic) {
            handler::handle(self.storage.as_ref(), machine, payload, ts_ms);
        }
    }
}
