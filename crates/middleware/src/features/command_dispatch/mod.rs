//! Command dispatch feature: subscribe ke command topic, validate vs index,
//! dedup vs storage current, publish ke PLC, persist via storage.
//!
//! # Apa yang dilakukan modul ini
//!
//! Untuk setiap pesan di `{base}/cmd/#`:
//! 1. Lookup topic di `cmd_index` — kalau tidak ada, **drop** (topic tidak dikenal di mapping).
//! 2. Parse JSON payload `{"value": ..., "request_id": "..."}` — kalau gagal, **error**.
//! 3. Coerce value ke tipe data entry (bool untuk control, float/integer untuk set) — kalau gagal, **error**.
//! 4. Baca state/nilai current dari ScyllaDB, **skip** (ack "skipped") kalau tidak berubah (dedup).
//!    Dedup hanya kalau current berumur <= [`STATE_MAX_AGE_MS`]. Current diisi readback
//!    PLC lewat `telemetry_ingest`, bukan oleh command, jadi command yang di-deny gate
//!    atau gagal ditulis tidak mengubahnya.
//! 5. Publish plain string payload ke topic tanpa `/cmd/` (forward ke edge-client).
//! 6. Catat command ke `*_history` di ScyllaDB.
//! 7. Kirim ack ke `{base}/ack/{request_id}`: `"ok"` / `"skipped"` / `"error"`.
//!
//! # Gate control — TIDAK dicek di layer ini
//!
//! Middleware **tidak** cek control gate Valkey sebelum publish. Gate di-enforce
//! sepenuhnya di sisi edge-client ([`crate::control_subscriber::handle_publish`]
//! di crate edge-client) via `HGETALL control` sebelum tiap Modbus write.
//!
//! Ini by design: middleware adalah bridge yang tidak tahu state fisik mesin.
//! Edge-client yang paling dekat dengan PLC dan paling tepat untuk enforce gate.

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

/// Maksimum concurrent command dispatch tasks. Melindungi dari command storm
/// (mis. client bug loop). Kalau penuh → warn + drop command.
const DISPATCH_CONCURRENCY: usize = 16;

/// Umur maksimum state readback yang dipakai untuk dedup. Lebih tua dari ini
/// (edge mati, entry tanpa readback, atau row lama dari sebelum readback ada)
/// dianggap tidak diketahui dan command selalu di-forward. Readback publish
/// tiap poll cycle (1s), jadi 5s memberi ruang beberapa cycle terlewat.
pub const STATE_MAX_AGE_MS: i64 = 5_000;

/// Service handle untuk command dispatching. Cheap to clone.
#[derive(Clone)]
pub struct CommandDispatchService {
    base_topic: Arc<String>,
    storage: Arc<dyn StorageRepo>,
    publisher: Arc<dyn MqttPublisher>,
    /// Lookup: command topic ({base}/cmd/{machine}/{tail}) → entry tujuan.
    cmd_index: Arc<HashMap<String, TopicEntry>>,
    publish_qos: i32,
    /// Semaphore untuk bound concurrent dispatch tasks.
    dispatch_sem: Arc<Semaphore>,
}

impl CommandDispatchService {
    /// Bangun command index dari TopicIndex (filter set + control).
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

    /// Wildcard untuk subscribe semua command topic.
    pub fn subscribe_pattern(&self) -> String {
        format!("{}/cmd/#", self.base_topic)
    }

    /// Apakah topic ini dimiliki commander (untuk routing di main loop).
    pub fn owns_topic(&self, topic: &str) -> bool {
        topic.starts_with(&format!("{}/cmd/", self.base_topic))
    }

    /// Process satu command. Spawn task supaya caller (MQTT loop) tidak block
    /// menunggu Scylla read. Production path.
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

    /// Test-friendly: bypass spawn, return Result agar bisa di-assert langsung.
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
