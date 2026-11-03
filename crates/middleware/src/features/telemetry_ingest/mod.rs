//! Telemetry ingest feature: take PLC values from MQTT, parse them, persist
//! them to storage by category and write through to the cache.
//!
//! - Monitoring goes to `telemetry`.
//! - Readback on `{base}/{machine}/state/{topic}` from edge-client goes to
//!   `setpoint_current` / `actuator_current`. This is the only source of `*_current`.
//! - `set/` and `control/` topics without `state/` are commands (forwarded by the
//!   middleware or published directly), not PLC state, so they are not recorded.

mod handler;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use crate::shared::cache::CacheRepo;
use crate::shared::storage::StorageRepo;
use crate::shared::time::now_ms;
use edge_shared::{Category, TopicEntry, TopicIndex, state_topic};

/// Service handle for telemetry ingest. Cheap to clone (only Arcs).
#[derive(Clone)]
pub struct TelemetryIngestService {
    index: Arc<TopicIndex>,
    /// Maps a readback state topic to its `set` / `control` entry.
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

    /// Process one incoming message, timestamped with the current clock.
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

    /// Same as `handle`, with an injected timestamp for tests.
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
