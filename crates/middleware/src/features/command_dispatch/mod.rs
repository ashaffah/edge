//! Command dispatch feature: subscribe to command topics, validate against
//! the index, dedup against the current state in storage, publish to the PLC
//! topic and persist through storage.
//!
//! For every message on `{base}/cmd/#`:
//! 1. Look the topic up in `cmd_index`. An unknown topic acks `"error"`.
//! 2. Parse the JSON payload `{"value": ..., "request_id": "..."}`. Invalid JSON gets no
//!    ack, since the `request_id` cannot be read.
//! 3. Coerce the value to the entry's data type (bool for control, float/integer for set).
//!    Failure acks `"error"`.
//! 4. Read the current state from ScyllaDB and ack `"skipped"` if the value is unchanged.
//!    Dedup only applies when the current row is at most [`STATE_MAX_AGE_MS`] old. The
//!    current row is filled by the PLC readback through `telemetry_ingest`, not by commands,
//!    so a command denied by the gate or a failed write does not change it.
//! 5. Publish the plain string payload to the topic without `/cmd/` (edge-client listens there).
//! 6. Record the command in the `*_history` table.
//! 7. Ack on `{base}/ack/{request_id}`: `"ok"`, `"skipped"` or `"error"`.
//!
//! # Control gate
//!
//! The middleware does not check the Valkey control gate before publishing.
//! edge-client enforces it (`control_subscriber::handle_publish` in the
//! edge-client crate) with `HGETALL control` before every Modbus write, since it
//! is the component next to the PLC.

mod coercion;
mod wire;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::shared::mqtt_client::MqttPublisher;
use crate::shared::storage::{ActuatorRow, SetpointRow, StorageRepo};
use crate::shared::time::now_ms;
use edge_shared::{Category, TopicEntry, TopicIndex};

use coercion::{format_value_for_plc, numbers_equal, value_as_bool, value_as_f64};
use wire::{AckPayload, CommandPayload};

/// Maximum concurrent command dispatch tasks. Guards against a command storm
/// (for example a client stuck in a loop). When full, the command is dropped
/// with a warning.
const DISPATCH_CONCURRENCY: usize = 16;

/// Maximum age of the readback state used for dedup. Anything older (edge
/// down, entry without readback, or a row from before readback existed) is
/// treated as unknown and the command is always forwarded. Readback is
/// published every poll cycle (1s), so 5s allows a few missed cycles.
pub const STATE_MAX_AGE_MS: i64 = 5_000;

/// Service handle for command dispatch. Cheap to clone.
#[derive(Clone)]
pub struct CommandDispatchService {
    base_topic: Arc<String>,
    storage: Arc<dyn StorageRepo>,
    publisher: Arc<dyn MqttPublisher>,
    /// Maps a command topic ({base}/cmd/{machine}/{tail}) to its target entry.
    cmd_index: Arc<HashMap<String, TopicEntry>>,
    publish_qos: i32,
    /// Bounds the number of concurrent dispatch tasks.
    dispatch_sem: Arc<Semaphore>,
}

impl CommandDispatchService {
    /// Build the command index from the TopicIndex (set and control entries only).
    pub fn new(
        base_topic: String,
        index: &TopicIndex,
        storage: Arc<dyn StorageRepo>,
        publisher: Arc<dyn MqttPublisher>,
        publish_qos: i32,
    ) -> Self {
        let cmd_index = build_cmd_index(&base_topic, index);
        info!(cmd_topics = cmd_index.len(), "command dispatch initialized");
        Self {
            base_topic: Arc::new(base_topic),
            storage,
            publisher,
            cmd_index: Arc::new(cmd_index),
            publish_qos,
            dispatch_sem: Arc::new(Semaphore::new(DISPATCH_CONCURRENCY)),
        }
    }

    /// Wildcard that matches every command topic.
    pub fn subscribe_pattern(&self) -> String {
        format!("{}/cmd/#", self.base_topic)
    }

    /// Whether this service owns the topic (for routing in the main loop).
    pub fn owns_topic(&self, topic: &str) -> bool {
        topic.starts_with(&format!("{}/cmd/", self.base_topic))
    }

    /// Process one command in a spawned task so the MQTT loop does not wait
    /// on the Scylla read. Production path.
    pub fn dispatch(&self, topic: String, payload: Vec<u8>) {
        let sem = Arc::clone(&self.dispatch_sem);
        let Ok(permit) = sem.try_acquire_owned() else {
            warn!(%topic, "dispatch slots full ({DISPATCH_CONCURRENCY}), dropping command");
            return;
        };
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = this.process(&topic, &payload).await {
                error!(%topic, "command processing failed: {e:#}");
                if let Ok(parsed) = serde_json::from_slice::<CommandPayload>(&payload)
                    && let Some(rid) = parsed.request_id.as_deref()
                {
                    this.send_ack(rid, "error", Some(format!("{e:#}"))).await;
                }
            }
        });
    }

    /// For tests: runs inline and returns the Result so it can be asserted.
    #[cfg(test)]
    pub async fn dispatch_sync(&self, topic: &str, payload: &[u8]) -> Result<()> {
        self.process(topic, payload).await
    }

    // ----- internal -----

    async fn process(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let entry = self
            .cmd_index
            .get(topic)
            .ok_or_else(|| anyhow!("unknown command topic"))?
            .clone();

        let cmd: CommandPayload = serde_json::from_slice(payload).context("parse command JSON")?;
        let request_id = cmd.request_id.clone();

        match entry.category {
            Category::Set => self.process_set(&entry, &cmd, request_id.as_deref()).await,
            Category::Control => {
                self.process_control(&entry, &cmd, request_id.as_deref())
                    .await
            }
            Category::Monitoring => bail!("monitoring topics are read-only"),
        }
    }

    async fn process_set(
        &self,
        entry: &TopicEntry,
        cmd: &CommandPayload,
        request_id: Option<&str>,
    ) -> Result<()> {
        let new_value = value_as_f64(&cmd.value, entry.data_type)?;

        let current = self
            .storage
            .read_setpoint_current(&entry.machine, &entry.key)
            .await
            .context("read setpoint_current")?;

        if let Some((existing, ts_ms)) = current
            && is_fresh(ts_ms)
            && numbers_equal(existing, new_value, entry.data_type)
        {
            debug!(
                machine = %entry.machine, key = %entry.key,
                existing, "setpoint unchanged, skipping publish"
            );
            if let Some(rid) = request_id {
                self.send_ack(rid, "skipped", Some("value unchanged".into()))
                    .await;
            }
            return Ok(());
        }

        let payload = format_value_for_plc(new_value, entry.data_type);
        self.publisher
            .publish(&entry.topic, payload.as_bytes(), self.publish_qos, false)
            .await
            .with_context(|| format!("publish to PLC topic {}", entry.topic))?;

        let row = SetpointRow {
            machine: entry.machine.clone(),
            key: entry.key.clone(),
            ts_ms: now_ms(),
            value: new_value,
            unit: entry.unit.clone(),
        };
        self.storage.record_setpoint_history(row);

        info!(
            machine = %entry.machine, key = %entry.key, value = new_value,
            "setpoint published"
        );
        if let Some(rid) = request_id {
            self.send_ack(rid, "ok", None).await;
        }
        Ok(())
    }

    async fn process_control(
        &self,
        entry: &TopicEntry,
        cmd: &CommandPayload,
        request_id: Option<&str>,
    ) -> Result<()> {
        let new_state = value_as_bool(&cmd.value)?;

        let current = self
            .storage
            .read_actuator_current(&entry.machine, &entry.key)
            .await
            .context("read actuator_current")?;

        if let Some((existing, ts_ms)) = current
            && is_fresh(ts_ms)
            && existing == new_state
        {
            debug!(
                machine = %entry.machine, key = %entry.key,
                existing, "actuator state unchanged, skipping publish"
            );
            if let Some(rid) = request_id {
                self.send_ack(rid, "skipped", Some("state unchanged".into()))
                    .await;
            }
            return Ok(());
        }

        let payload = if new_state { "1" } else { "0" };
        self.publisher
            .publish(&entry.topic, payload.as_bytes(), self.publish_qos, false)
            .await
            .with_context(|| format!("publish to PLC topic {}", entry.topic))?;

        let row = ActuatorRow {
            machine: entry.machine.clone(),
            key: entry.key.clone(),
            ts_ms: now_ms(),
            state: new_state,
        };
        self.storage.record_actuator_history(row);

        info!(
            machine = %entry.machine, key = %entry.key, state = new_state,
            "control published"
        );
        if let Some(rid) = request_id {
            self.send_ack(rid, "ok", None).await;
        }
        Ok(())
    }

    async fn send_ack(&self, request_id: &str, status: &str, detail: Option<String>) {
        let ack_topic = format!("{}/ack/{}", self.base_topic, request_id);
        let body = AckPayload {
            request_id,
            status,
            detail,
        };
        let payload = match serde_json::to_vec(&body) {
            Ok(p) => p,
            Err(e) => {
                error!("serialize ack failed: {e:#}");
                return;
            }
        };
        if let Err(e) = self
            .publisher
            .publish(&ack_topic, &payload, self.publish_qos, false)
            .await
        {
            warn!(%ack_topic, "ack publish failed: {e:#}");
        }
    }
}

fn is_fresh(ts_ms: i64) -> bool {
    now_ms() - ts_ms <= STATE_MAX_AGE_MS
}

fn build_cmd_index(base_topic: &str, index: &TopicIndex) -> HashMap<String, TopicEntry> {
    let mut cmd_index = HashMap::new();
    for entry in &index.entries {
        if !matches!(entry.category, Category::Set | Category::Control) {
            continue;
        }
        let machine_prefix = format!("{}/{}/", base_topic, entry.machine);
        let tail = match entry.topic.strip_prefix(&machine_prefix) {
            Some(t) => t,
            None => {
                warn!(topic = %entry.topic, "topic does not start with expected machine prefix, skipping");
                continue;
            }
        };
        let cmd_topic = format!("{}/cmd/{}/{}", base_topic, entry.machine, tail);
        cmd_index.insert(cmd_topic, entry.clone());
    }
    cmd_index
}
