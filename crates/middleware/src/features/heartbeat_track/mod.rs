//! Heartbeat track feature: subscribe ke `{base}/{machine}/heartbeat` per
//! mesin, persist status (connected/disconnected) ke storage.
//!
//! Edge-client publish:
//!   - Periodik (mis. tiap 10s): `{"status": "connected"}`
//!   - Sebagai LWT (auto-publish saat client disconnect): `{"status": "disconnected"}`
//!
//! Middleware route topic ini via `owns_topic` di routing main.rs, sebelum
//! fall-through ke `telemetry_ingest` (yang akan reject karena tidak ada di
//! TopicIndex).

mod handler;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::shared::mqtt_client::MqttPublisher;
use crate::shared::storage::{HeartbeatRow, StorageRepo};
use crate::shared::time::now_ms;

const HEARTBEAT_SUFFIX: &str = "/heartbeat";

/// Payload yang di-publish (retained) saat staleness menandai mesin offline,
/// supaya retained MQTT konsisten dengan history Scylla. Match contract
/// edge-client LWT.
const DISCONNECTED_PAYLOAD: &[u8] = br#"{"status":"disconnected"}"#;

/// Sebuah mesin dianggap offline kalau tidak ada heartbeat `connected` lebih
/// lama dari ini. Edge-client publish tiap 1 detik, jadi 5s = ~5 beat hilang.
/// LWT meng-cover machine_id primer secara instan; staleness ini meng-cover
/// SEMUA mesin (termasuk mesin sekunder di instance multi-PLC yang tidak punya
/// LWT sendiri). Promote ke env var kalau perlu per-deploy.
const OFFLINE_AFTER: Duration = Duration::from_secs(5);
/// Seberapa sering scan liveness map untuk deteksi mesin basi.
const CHECK_EVERY: Duration = Duration::from_secs(2);

/// State liveness per mesin untuk deteksi offline berbasis staleness.
struct Liveness {
    /// Kapan terakhir terima heartbeat `connected`.
    last_seen: Instant,
    /// Apakah saat ini dianggap online. Dipakai supaya transisi → offline
    /// hanya di-record sekali (tidak spam disconnected tiap scan).
    online: bool,
}

/// Service handle. Cheap to clone (cuma Arc).
#[derive(Clone)]
pub struct HeartbeatTrackService {
    /// Prefix `{base_topic}/` — pre-formatted untuk fast prefix check.
    base_prefix: Arc<String>,
    /// Set mesin yang valid (dari `MQTT_MACHINES`). Topic dari mesin di luar
    /// set ini di-reject di `owns_topic`.
    machines: Arc<HashSet<String>>,
    storage: Arc<dyn StorageRepo>,
    /// Publisher untuk menimpa retained heartbeat jadi `disconnected` saat
    /// staleness — supaya retained MQTT tidak stuck `connected` untuk mesin
    /// sekunder yang tidak punya LWT sendiri.
    publisher: Arc<dyn MqttPublisher>,
    publish_qos: i32,
    /// Last-seen tracker per mesin untuk staleness offline detection.
    liveness: Arc<Mutex<HashMap<String, Liveness>>>,
}

impl HeartbeatTrackService {
    pub fn new(
        base_topic: &str,
        machines: &[String],
        storage: Arc<dyn StorageRepo>,
        publisher: Arc<dyn MqttPublisher>,
        publish_qos: i32,
    ) -> Self {
        Self {
            base_prefix: Arc::new(format!("{}/", base_topic)),
            machines: Arc::new(machines.iter().cloned().collect()),
            storage,
            publisher,
            publish_qos,
            liveness: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Update liveness tracker dari hasil parse heartbeat.
    fn update_liveness(&self, machine: &str, connected: bool) {
        let mut map = self.liveness.lock().unwrap();
        let entry = map.entry(machine.to_string()).or_insert(Liveness {
            last_seen: Instant::now(),
            online: connected,
        });
        entry.last_seen = Instant::now();
        entry.online = connected;
    }

    /// Loop background: tandai mesin `disconnected` kalau heartbeat-nya basi
    /// (> `OFFLINE_AFTER`). Hanya bertindak pada mesin yang pernah online, dan
    /// hanya sekali per transisi → tidak spam. Embed di `tokio::spawn` di main.
    pub async fn run_staleness_monitor(self) {
        let mut ticker = tokio::time::interval(CHECK_EVERY);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now = Instant::now();
            // Kumpulkan mesin basi di dalam lock, lalu lepas lock sebelum
            // record (record_heartbeat fire-and-forget, tapi tetap jaga supaya
            // tidak ada lock yang hidup lewat boundary kerja lain).
            let mut stale: Vec<String> = Vec::new();
            {
                let mut map = self.liveness.lock().unwrap();
                for (machine, live) in map.iter_mut() {
                    if live.online && now.duration_since(live.last_seen) > OFFLINE_AFTER {
                        live.online = false;
                        stale.push(machine.clone());
                    }
                }
            }
            for machine in stale {
                warn!(%machine, "heartbeat stale > {OFFLINE_AFTER:?}, marking disconnected");
                self.storage.record_heartbeat(HeartbeatRow {
                    machine: machine.clone(),
                    status: "disconnected".to_string(),
                    ts_ms: now_ms(),
                });
                // Timpa retained heartbeat di broker → subscriber MQTT baru
                // (dashboard) tidak lihat `connected` basi. Mesin primer juga
                // dapat ini (redundan dengan LWT, tapi harmless).
                let topic = format!("{}{}/heartbeat", self.base_prefix, machine);
                if let Err(e) = self
                    .publisher
                    .publish(&topic, DISCONNECTED_PAYLOAD, self.publish_qos, true)
                    .await
                {
                    warn!(%topic, "publish retained disconnected gagal: {e:#}");
                }
            }
        }
    }

    /// True kalau topic ini milik service (untuk routing di main.rs).
    /// Topic shape valid: `{base}/{machine}/heartbeat` dengan machine ∈ MQTT_MACHINES.
    pub fn owns_topic(&self, topic: &str) -> bool {
        self.extract_machine(topic).is_some()
    }

    /// Ekstrak nama machine dari topic. None kalau topic tidak match shape
    /// atau machine tidak dikenal.
    fn extract_machine<'a>(&self, topic: &'a str) -> Option<&'a str> {
        let middle = topic
            .strip_prefix(self.base_prefix.as_str())?
            .strip_suffix(HEARTBEAT_SUFFIX)?;
        if self.machines.contains(middle) {
            Some(middle)
        } else {
            None
        }
    }

    /// Proses satu pesan masuk. Caller (main loop) sudah panggil `owns_topic`
    /// untuk filter; di sini kita re-extract machine (tidak ekspensif).
    pub fn handle(&self, topic: &str, payload: &str) {
        if let Some(machine) = self.extract_machine(topic)
            && let Some(connected) =
                handler::handle(self.storage.as_ref(), machine, payload, now_ms())
        {
            self.update_liveness(machine, connected);
        }
        // Else: caller tidak seharusnya panggil ini untuk topic foreign;
        // silently drop kalau kejadian (defense in depth).
    }

    /// Variant test-friendly: inject timestamp.
    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        if let Some(machine) = self.extract_machine(topic)
            && let Some(connected) = handler::handle(self.storage.as_ref(), machine, payload, ts_ms)
        {
            self.update_liveness(machine, connected);
        }
    }
}
