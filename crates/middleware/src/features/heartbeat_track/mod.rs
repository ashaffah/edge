//! Heartbeat track feature: subscribe to `{base}/{machine}/heartbeat` for each
//! machine and persist the status (connected/disconnected) to storage.
//!
//! edge-client publishes:
//!   - periodically: `{"status": "connected"}`
//!   - as its LWT, sent by the broker when the client drops: `{"status": "disconnected"}`
//!
//! main.rs routes these topics here through `owns_topic` before falling
//! through to `telemetry_ingest`, which would reject them because they are
//! not in the TopicIndex.

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

/// Payload published (retained) when staleness marks a machine offline, so the
/// retained MQTT value matches the Scylla history. Same payload as the
/// edge-client LWT.
const DISCONNECTED_PAYLOAD: &[u8] = br#"{"status":"disconnected"}"#;

/// A machine is considered offline when no `connected` heartbeat arrived for
/// longer than this. edge-client publishes every second, so 5s is about 5
/// missed beats. The LWT covers the primary machine_id immediately; this
/// covers every machine, including secondary machines of a multi-PLC instance
/// that have no LWT of their own. Move to an env var if deployments need
/// different values.
const OFFLINE_AFTER: Duration = Duration::from_secs(5);
/// How often the liveness map is scanned for stale machines.
const CHECK_EVERY: Duration = Duration::from_secs(2);

/// Per-machine liveness state for staleness-based offline detection.
struct Liveness {
    /// When the last `connected` heartbeat arrived.
    last_seen: Instant,
    /// Whether the machine is currently considered online, so the transition to
    /// offline is recorded once instead of on every scan.
    online: bool,
}

/// Service handle. Cheap to clone (only Arcs).
#[derive(Clone)]
pub struct HeartbeatTrackService {
    /// Pre-formatted `{base_topic}/` prefix for a fast prefix check.
    base_prefix: Arc<String>,
    /// Known machines (from the mapping). `owns_topic` rejects topics of any
    /// other machine.
    machines: Arc<HashSet<String>>,
    storage: Arc<dyn StorageRepo>,
    /// Used to overwrite the retained heartbeat with `disconnected` on
    /// staleness, so it does not stay `connected` for secondary machines that
    /// have no LWT of their own.
    publisher: Arc<dyn MqttPublisher>,
    publish_qos: i32,
    /// Per-machine last-seen tracker for staleness detection.
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

    /// Update the liveness tracker from a parsed heartbeat.
    fn update_liveness(&self, machine: &str, connected: bool) {
        let mut map = self.liveness.lock().unwrap();
        let entry = map.entry(machine.to_string()).or_insert(Liveness {
            last_seen: Instant::now(),
            online: connected,
        });
        entry.last_seen = Instant::now();
        entry.online = connected;
    }

    /// Background loop: mark a machine `disconnected` when its heartbeat is
    /// older than `OFFLINE_AFTER`. Only acts on machines that were online, and
    /// once per transition. main.rs runs it in `tokio::spawn`.
    pub async fn run_staleness_monitor(self) {
        let mut ticker = tokio::time::interval(CHECK_EVERY);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now = Instant::now();
            // Collect stale machines under the lock and release it before
            // recording, so the lock is never held across other work.
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
                // Overwrite the retained heartbeat so a new MQTT subscriber
                // (dashboard) does not see a stale `connected`. Primary machines
                // get this too, which duplicates the LWT but does no harm.
                let topic = format!("{}{}/heartbeat", self.base_prefix, machine);
                if let Err(e) = self
                    .publisher
                    .publish(&topic, DISCONNECTED_PAYLOAD, self.publish_qos, true)
                    .await
                {
                    warn!(%topic, "publish retained disconnected failed: {e:#}");
                }
            }
        }
    }

    /// True if this service owns the topic (for routing in main.rs).
    /// Valid shape: `{base}/{machine}/heartbeat` where machine is in the mapping.
    pub fn owns_topic(&self, topic: &str) -> bool {
        self.extract_machine(topic).is_some()
    }

    /// Extract the machine from the topic. None if the shape does not match or
    /// the machine is unknown.
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

    /// Process one incoming message. The caller already filtered with
    /// `owns_topic`; the machine is extracted again here (cheap).
    pub fn handle(&self, topic: &str, payload: &str) {
        if let Some(machine) = self.extract_machine(topic)
            && let Some(connected) =
                handler::handle(self.storage.as_ref(), machine, payload, now_ms())
        {
            self.update_liveness(machine, connected);
        }
        // The caller should not pass a foreign topic. Drop it if it does.
    }

    /// Same as `handle`, with an injected timestamp for tests.
    #[cfg(test)]
    pub fn handle_at(&self, topic: &str, payload: &str, ts_ms: i64) {
        if let Some(machine) = self.extract_machine(topic)
            && let Some(connected) = handler::handle(self.storage.as_ref(), machine, payload, ts_ms)
        {
            self.update_liveness(machine, connected);
        }
    }
}
