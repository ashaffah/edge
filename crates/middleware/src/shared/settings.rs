//! Runtime configuration: env (a `.env` file is loaded if present) plus the
//! JSON mapping.
//!
//! `main.rs` builds `Settings` once at startup and passes the sub-structs
//! (`MqttSettings`, `ScyllaSettings`) to the components that need them.
//! Nothing outside this file reads env variables.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, de::DeserializeOwned};
use std::{collections::HashSet, env, fs, path::Path, time::Duration};

use edge_shared::{MiddlewareMapping, MwDevice};

const MAPPING_BASE_FILE: &str = "_base.json";

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Settings {
    pub mqtt: MqttSettings,
    pub base_topic: String,
    pub machines: Vec<String>,
    pub mapping: MiddlewareMapping,
    pub scylla: ScyllaSettings,
    pub cache: CacheSettings,
}

#[derive(Debug, Clone)]
pub struct MqttSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub client_id: String,
    pub protocol: String,
    pub ssl_tls: bool,
    pub websocket_path: Option<String>,
    pub qos: i32,
    pub keepalive_secs: u64,
    pub publish_qos: i32,
    pub cmd_qos: i32,
}

#[derive(Debug, Clone)]
pub struct ScyllaSettings {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub consistency: String,
    pub telemetry_channel_capacity: usize,
    pub telemetry_batch_max_rows: usize,
    pub telemetry_flush_interval: Duration,
    pub telemetry_concurrency: usize,
}

/// Cache settings. With an empty `url` the middleware uses `NoOpCache`
/// (cache disabled, useful for dev and CI without a running Valkey).
#[derive(Debug, Clone)]
pub struct CacheSettings {
    pub url: String,
    pub key_prefix: String,
    pub telemetry_ttl_secs: u64,
    pub state_ttl_secs: u64,
}

impl CacheSettings {
    pub fn enabled(&self) -> bool {
        !self.url.is_empty()
    }
}

impl Default for ScyllaSettings {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:9042".into()],
            keyspace: "iot_middleware".into(),
            username: None,
            password: None,
            consistency: "LocalOne".into(),
            telemetry_channel_capacity: 10_000,
            telemetry_batch_max_rows: 100,
            telemetry_flush_interval: Duration::from_millis(200),
            telemetry_concurrency: 32,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let mqtt = load_mqtt_settings()?;
        let mapping_path = optional("MQTT_MAPPING_PATH", "./mqtt-mapping.json");
        let mapping = load_mapping(&mapping_path)?;
        let base_topic = mapping.base_topic.clone();
        let machines: Vec<String> = mapping
            .devices
            .iter()
            .map(|d| format!("{}/{}", d.location, d.name))
            .collect();
        let scylla = load_scylla_settings()?;
        let cache = load_cache_settings()?;

        Ok(Self {
            mqtt,
            base_topic,
            machines,
            mapping,
            scylla,
            cache,
        })
    }

    pub fn broker_uri(&self) -> String {
        format!(
            "{}://{}:{}",
            self.mqtt.protocol, self.mqtt.host, self.mqtt.port
        )
    }

    /// Telemetry subscribe patterns, one wildcard per machine.
    pub fn subscribe_patterns(&self) -> Vec<String> {
        self.machines
            .iter()
            .map(|m| format!("{}/{}/#", self.base_topic, m))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

fn load_mqtt_settings() -> Result<MqttSettings> {
    Ok(MqttSettings {
        host: required("MQTT_HOST")?,
        port: required("MQTT_PORT")?
            .parse()
            .context("MQTT_PORT must be u16")?,
        username: required("MQTT_USERNAME")?,
        password: required("MQTT_PASSWORD")?,
        client_id: required("MQTT_CLIENT_ID")?,
        protocol: optional("MQTT_PROTOCOL", "mqtt"),
        ssl_tls: optional("MQTT_TLS_SSL", "false")
            .parse()
            .context("MQTT_TLS_SSL must be bool")?,
        websocket_path: optional("MQTT_WEBSOCKET_PATH", "/mqtt").into(),
        qos: optional("MQTT_QOS", "1")
            .parse()
            .context("MQTT_QOS must be i32")?,
        keepalive_secs: optional("MQTT_KEEPALIVE", "60")
            .parse()
            .context("MQTT_KEEPALIVE must be u64")?,
        publish_qos: optional("MQTT_PUBLISH_QOS", "1")
            .parse()
            .context("MQTT_PUBLISH_QOS must be i32")?,
        cmd_qos: optional("MQTT_CMD_QOS", "1")
            .parse()
            .context("MQTT_CMD_QOS must be i32")?,
    })
}

fn load_scylla_settings() -> Result<ScyllaSettings> {
    let nodes = parse_csv_with_default("SCYLLA_NODES", "127.0.0.1:9042");
    if nodes.is_empty() {
        bail!("SCYLLA_NODES is empty");
    }
    Ok(ScyllaSettings {
        nodes,
        keyspace: optional("SCYLLA_KEYSPACE", "iot_middleware"),
        username: env::var("SCYLLA_USERNAME").ok().filter(|s| !s.is_empty()),
        password: env::var("SCYLLA_PASSWORD").ok().filter(|s| !s.is_empty()),
        consistency: optional("SCYLLA_CONSISTENCY", "LocalOne"),
        telemetry_channel_capacity: optional("SCYLLA_TELEMETRY_CHANNEL", "10000")
            .parse()
            .context("SCYLLA_TELEMETRY_CHANNEL must be usize")?,
        telemetry_batch_max_rows: optional("SCYLLA_TELEMETRY_BATCH", "100")
            .parse()
            .context("SCYLLA_TELEMETRY_BATCH must be usize")?,
        telemetry_flush_interval: Duration::from_millis(
            optional("SCYLLA_TELEMETRY_FLUSH_MS", "200")
                .parse()
                .context("SCYLLA_TELEMETRY_FLUSH_MS must be u64")?,
        ),
        telemetry_concurrency: optional("SCYLLA_TELEMETRY_CONCURRENCY", "32")
            .parse()
            .context("SCYLLA_TELEMETRY_CONCURRENCY must be usize")?,
    })
}

fn load_cache_settings() -> Result<CacheSettings> {
    // Empty URL means cache disabled (NoOpCache).
    let url = optional("CACHE_URL", "");
    Ok(CacheSettings {
        url,
        key_prefix: optional("CACHE_KEY_PREFIX", "iot:middleware"),
        telemetry_ttl_secs: optional("CACHE_TELEMETRY_TTL_SECS", "3600")
            .parse()
            .context("CACHE_TELEMETRY_TTL_SECS must be u64")?,
        state_ttl_secs: optional("CACHE_STATE_TTL_SECS", "86400")
            .parse()
            .context("CACHE_STATE_TTL_SECS must be u64")?,
    })
}

/// `path` is either a single file (`MiddlewareMapping`) or a directory where
/// `_base.json` holds `base_topic` and every other `*.json` holds one `MwDevice`.
fn load_mapping(path: impl AsRef<Path>) -> Result<MiddlewareMapping> {
    let path = path.as_ref();
    let mapping = if path.is_dir() {
        load_mapping_dir(path)?
    } else {
        read_json(path)?
    };

    // Per-machine files are usually made by copying another one. A location/name
    // left unchanged must fail at startup instead of silently sharing topics.
    let mut seen = HashSet::new();
    for d in &mapping.devices {
        if !seen.insert((d.location.as_str(), d.name.as_str())) {
            bail!(
                "duplicate device {}/{} in mapping: {}",
                d.location,
                d.name,
                path.display()
            );
        }
    }
    Ok(mapping)
}

#[derive(Deserialize)]
struct MappingBase {
    base_topic: String,
    #[serde(default)]
    poll_interval_ms: Option<u64>,
}

fn load_mapping_dir(dir: &Path) -> Result<MiddlewareMapping> {
    let base: MappingBase = read_json(&dir.join(MAPPING_BASE_FILE))?;

    let mut device_files = Vec::new();
    for entry in
        fs::read_dir(dir).with_context(|| format!("read mapping dir: {}", dir.display()))?
    {
        let p = entry?.path();
        let is_json = p.extension().is_some_and(|e| e == "json");
        if is_json && p.file_name().is_some_and(|n| n != MAPPING_BASE_FILE) {
            device_files.push(p);
        }
    }
    if device_files.is_empty() {
        bail!("no device *.json in mapping dir: {}", dir.display());
    }
    // read_dir has no guaranteed order. Sort by name so logs and the index are stable.
    device_files.sort();

    let devices = device_files
        .iter()
        .map(|p| read_json(p))
        .collect::<Result<Vec<MwDevice>>>()?;

    Ok(MiddlewareMapping {
        base_topic: base.base_topic,
        poll_interval_ms: base.poll_interval_ms,
        devices,
    })
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read mapping file: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse mapping JSON: {}", path.display()))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn optional(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_csv_with_default(name: &str, default: &str) -> Vec<String> {
    optional(name, default)
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const DEVICE_AREA1: &str = r#"{ "location": "area1", "name": "machine_a",
        "monitoring": [{ "key": "current_motor", "label": "Motor Current", "topic": "current/motor", "type": "float" }] }"#;

    fn temp_dir(name: &str, files: &[(&str, &str)]) -> PathBuf {
        let dir = env::temp_dir().join(format!("iot-mw-mapping-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        for (file, content) in files {
            fs::write(dir.join(file), content).unwrap();
        }
        dir
    }

    #[test]
    fn deploy_mapping_dir_loads() {
        let dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/middleware/mqtt-mapping");
        let m = load_mapping(&dir).unwrap();
        assert_eq!(m.base_topic, "acme/site");
        assert_eq!(m.devices.len(), 3);
    }

    #[test]
    fn dir_merges_devices_sorted_by_file_name() {
        let area2 = DEVICE_AREA1.replace("area1", "area2");
        let dir = temp_dir(
            "merge",
            &[
                (
                    "_base.json",
                    r#"{ "base_topic": "acme/site", "poll_interval_ms": 1000 }"#,
                ),
                ("area2.json", &area2),
                ("area1.json", DEVICE_AREA1),
                ("README.md", "not a mapping"),
            ],
        );
        let m = load_mapping(&dir).unwrap();
        assert_eq!(m.poll_interval_ms, Some(1000));
        let locations: Vec<_> = m.devices.iter().map(|d| d.location.as_str()).collect();
        assert_eq!(locations, ["area1", "area2"]);
    }

    #[test]
    fn dir_rejects_duplicate_device() {
        let dir = temp_dir(
            "dup",
            &[
                ("_base.json", r#"{ "base_topic": "acme/site" }"#),
                ("a.json", DEVICE_AREA1),
                ("b.json", DEVICE_AREA1),
            ],
        );
        let err = load_mapping(&dir).unwrap_err().to_string();
        assert!(err.contains("duplicate device area1/machine_a"), "{err}");
    }

    #[test]
    fn dir_without_devices_fails() {
        let dir = temp_dir(
            "empty",
            &[("_base.json", r#"{ "base_topic": "acme/site" }"#)],
        );
        assert!(load_mapping(&dir).is_err());
    }

    #[test]
    fn dir_without_base_fails() {
        let dir = temp_dir("nobase", &[("area1.json", DEVICE_AREA1)]);
        let err = format!("{:#}", load_mapping(&dir).unwrap_err());
        assert!(err.contains("_base.json"), "{err}");
    }

    #[test]
    fn single_file_still_loads() {
        let json = format!(r#"{{ "base_topic": "acme/site", "devices": [{DEVICE_AREA1}] }}"#);
        let dir = temp_dir("single", &[("mqtt-mapping.json", &json)]);
        let m = load_mapping(dir.join("mqtt-mapping.json")).unwrap();
        assert_eq!(m.devices.len(), 1);
    }
}
