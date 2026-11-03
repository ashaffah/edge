//! Telemetry ingest feature: terima MQTT message dari PLC, parse, persist
//! ke storage per kategori, dan write-through ke cache untuk current state mirror.
//!
//! - Monitoring: `telemetry`.
//! - Readback `{base}/{machine}/state/{topic}` dari edge-client: `setpoint_current` /
//!   `actuator_current`. Ini satu-satunya sumber `*_current`.
//! - Topic `set/` / `control/` tanpa `state/` adalah command (forward middleware atau
//!   publish langsung), bukan state PLC, jadi tidak dicatat.

mod handler;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use crate::shared::cache::CacheRepo;
use crate::shared::storage::StorageRepo;
use crate::shared::time::now_ms;
use edge_shared::{Category, TopicEntry, TopicIndex, state_topic};

/// Service handle untuk telemetry ingestion. Cheap to clone (cuma Arc).
#[derive(Clone)]
pub struct TelemetryIngestService {
    index: Arc<TopicIndex>,
    /// State topic readback ke entry `set` / `control` asalnya.
    state_index: Arc<HashMap<String, TopicEntry>>,
    storage: Arc<dyn StorageRepo>,
    cache: Arc<dyn CacheRepo>,
}

impl TelemetryIngestService {
    pub fn new(
        base_topic: &str,
        index: Arc<TopicIndex>,
        storage: Arc<dyn StorageRepo>,
        cache: Arc<dyn CacheRepo>,
    ) -> Self {
        let state_index = Arc::new(build_state_index(base_topic, &index));
        Self {
            index,
            state_index,
            storage,
            cache,
        }
    }

    /// Proses satu pesan masuk. Timestamp diambil dari clock saat ini.
    pub fn handle(&self, topic: &str, payload: &str) {
        handler::handle(
            &self.index,
            &self.state_index,
            self.storage.as_ref(),
            self.cache.as_ref(),
            topic,
            payload,
            now_ms(),
        );
    }

    /// Variant test-friendly: timestamp di-inject.
    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        handler::handle(
            &self.index,
            &self.state_index,
            self.storage.as_ref(),
            self.cache.as_ref(),
            topic,
            payload,
            ts_ms,
        );
    }
}

fn build_state_index(base_topic: &str, index: &TopicIndex) -> HashMap<String, TopicEntry> {
    index
        .entries
        .iter()
        .filter(|e| matches!(e.category, Category::Set | Category::Control))
        .filter_map(|e| {
            let prefix = format!("{}/{}/", base_topic, e.machine);
            let param_topic = e.topic.strip_prefix(&prefix)?;
            Some((state_topic(base_topic, &e.machine, param_topic), e.clone()))
        })
        .collect()
}
