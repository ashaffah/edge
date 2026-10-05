//! Edge-client runtime configuration: loaded from env (`.env` auto-loaded if
//! present) + the JSON mapping format (`example-mapping.json` with a `devices`
//! array).
//!
//! Edge-client is **1 instance per machine**. Each deployment has its own
//! `.env` with MQTT credentials; the machine_id and Modbus connection come
//! entirely from the JSON mapping (there are no `MACHINE_ID` / `MODBUS_*` env
//! vars).
//!
//! Edge-client does no persistence at all — all data is sent to the MQTT broker
//! and consumed on the other side. `CACHE_URL` / `CONTROL_GATE_KEY` are read for
//! control-command gate authorization — see [`crate::control_gate`] for details.

use anyhow::{Context, Result, bail};
use std::{env, fs, path::Path, time::Duration};

use crate::shared::{DeviceConnection, DeviceMapping, DeviceType};

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Settings {
    pub mqtt: MqttSettings,
    pub base_topic: String,
    pub machine_id: String,
    pub mapping: DeviceMapping,
    /// `None` if the mapping has no PLC device — this instance only manages
    /// weigher scales (no Modbus polling/control).
    pub modbus: Option<ModbusSettings>,
    pub control_gate: ControlGateSettings,
}

#[derive(Debug, Clone)]
pub struct MqttSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub client_id: String,
    pub keepalive_secs: u64,
    /// QoS for publishing telemetry + heartbeat + resources to the broker.
    pub publish_qos: u8,
    /// Transport scheme: `mqtt` (plain TCP, default), `mqtts` (MQTT over TLS),
    /// `ws` (MQTT over WebSocket), `wss` (MQTT over WebSocket Secure).
    /// Mapped to the rumqttc Transport variant via a manual switch in
    /// `mqtt::build_client`. An EMQX broker typically uses `wss` on port 443 or
    /// `mqtts` on 8883.
    pub protocol: String,
    pub ssl_tls: bool,
    pub websocket_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ModbusSettings {
    pub transport: Transport,
    pub unit_id: u8,
    /// Polling interval. Default 1s (1 Hz per param).
    pub poll_interval: Duration,
    /// RTU timing: request timeout + inter-frame delay. Loaded from env for all
    /// transports (validation still runs even when transport=tcp).
    pub rtu_timings: crate::modbus_actor::RtuTimings,
}

/// Three transports to the PLC, chosen per deployment via the JSON mapping's
/// `connection.type`.
///
/// 1 edge instance = 1 PLC = 1 transport. Whether Ethernet, RS-485 via a
/// converter, or direct serial; the operator picks based on the field hardware.
/// The JSON mapping (`connection` in each device entry) determines the
/// transport per device.
#[derive(Debug, Clone)]
pub enum Transport {
    /// Standard Modbus TCP (MBAP header). Default port 502. Ethernet PLC.
    Tcp { host: String, port: u16 },
    /// Modbus RTU frames (with CRC) encapsulated in plain TCP, without MBAP.
    /// The default mode of RS-485↔Ethernet converters (Moxa NPort, USR-TCP232,
    /// Waveshare, Elfin). RFC 2217 (Telnet-negotiated serial) is **not**
    /// supported here — add a new variant later if needed.
    RtuOverTcp { host: String, port: u16 },
    /// Modbus RTU directly to a serial port. Path = the device node
    /// (`/dev/ttyUSB0`, `/dev/ttyS0`, `COM3`, etc.). A USB-RS485 adapter appears
    /// as a normal serial port via the CDC driver — no separate mode needed.
    RtuSerial {
        path: String,
        baud: u32,
        parity: SerialParity,
        stop_bits: SerialStopBits,
        data_bits: SerialDataBits,
    },
}

impl Transport {
    /// Human-readable display for logs. Not parseable back — purely a debug aid.
    pub fn display(&self) -> String {
        match self {
            Self::Tcp { host, port } => format!("tcp://{host}:{port}"),
            Self::RtuOverTcp { host, port } => format!("rtu-over-tcp://{host}:{port}"),
            Self::RtuSerial {
                path,
                baud,
                parity,
                stop_bits,
                data_bits,
            } => format!("rtu-serial://{path} ({baud} {data_bits:?}{parity:?}{stop_bits:?})"),
        }
    }
}

/// Parity bit for RTU serial. Standard Modbus RTU = `Even` (most PLCs), but in
/// practice many vendors use `None`. Our default is `None` because it matches
/// the broader industry assumption + the configuration most commonly seen in
/// the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialParity {
    None,
    Even,
    Odd,
}

/// Stop bits for RTU serial. `One` is standard; `Two` is used when parity=None
/// in some strict specs (compensating for the lost parity for frame integrity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialStopBits {
    One,
    Two,
}

/// Data bits per character. Modbus RTU = always 8-bit. `Seven` is exposed only
/// for odd legacy devices — should not be needed in typical deployments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialDataBits {
    Seven,
    Eight,
}

/// Gate authorization configuration for control commands.
///
/// `url == None` → gate disabled, all control denied. Deliberately does not
/// auto-allow when empty: running without Valkey/Redis stays safe (no
/// accidental writes).
#[derive(Debug, Clone)]
pub struct ControlGateSettings {
    /// Format: `redis://[user:pass@]host:port[/db]`. `None` = disabled.
    pub url: Option<String>,
    /// Hash key in Valkey. Default `"control"`.
    pub hash_key: String,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let mqtt = load_mqtt_settings()?;
        let mapping_path = optional("MQTT_MAPPING_PATH", "./mapping.json");
        let mapping = load_mapping_file(&mapping_path)?;

        let base_topic = mapping.base_topic.clone();
        let machine_id = derive_machine_id(&mapping)?;
        let modbus = load_modbus_settings_opt(&mapping)?;
        let control_gate = load_control_gate_settings();

        Ok(Self {
            mqtt,
            base_topic,
            machine_id,
            mapping,
            modbus,
            control_gate,
        })
    }

    pub fn broker_uri(&self) -> String {
        format!("{}:{}", self.mqtt.host, self.mqtt.port)
    }
}

/// Derive machine_id from the DeviceMapping.
///
/// Priority: (1) PLC → (2) Slave → (3) Weigher.
/// There must be at least one device entry for machine_id to be derivable.
fn derive_machine_id(mapping: &DeviceMapping) -> Result<String> {
    // 1. First PLC
    if let Some(d) = mapping
        .devices
        .iter()
        .find(|d| d.device_type == DeviceType::Master)
    {
        return Ok(format!("{}/{}", d.location, d.name));
    }
    // 2. First slave
    if let Some(d) = mapping
        .devices
        .iter()
        .find(|d| d.device_type == DeviceType::Slave)
    {
        return Ok(format!("{}/{}", d.location, d.name));
    }
    // 3. First weigher (scale-only instance, no Modbus)
    if let Some(d) = mapping
        .devices
        .iter()
        .find(|d| d.device_type == DeviceType::Weigher)
    {
        return Ok(format!("{}/{}", d.location, d.name));
    }
    bail!(
        "machine_id cannot be derived: the JSON mapping has no device. \
         Add at least one entry under 'devices'."
    )
}

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

fn load_mqtt_settings() -> Result<MqttSettings> {
    let protocol = optional("MQTT_PROTOCOL", "mqtt").to_ascii_lowercase();
    match protocol.as_str() {
        "mqtt" | "mqtts" | "ws" | "wss" => {}
        other => bail!(
            "MQTT_PROTOCOL '{other}' is not recognized. Choose one of: \
             'mqtt' (plain TCP), 'mqtts' (MQTT over TLS, usually port 8883), \
             'ws' (plain WebSocket), 'wss' (WebSocket over TLS, usually port 443)"
        ),
    }
    Ok(MqttSettings {
        host: required("MQTT_HOST")?,
        port: required("MQTT_PORT")?
            .parse()
            .context("MQTT_PORT must be u16")?,
        username: required("MQTT_USERNAME")?,
        password: required("MQTT_PASSWORD")?,
        client_id: required("MQTT_CLIENT_ID")?,
        keepalive_secs: optional("MQTT_KEEPALIVE", "60")
            .parse()
            .context("MQTT_KEEPALIVE must be u64")?,
        publish_qos: optional("MQTT_PUBLISH_QOS", "1")
            .parse()
            .context("MQTT_PUBLISH_QOS must be u8 (0|1|2)")?,
        protocol,
        ssl_tls: optional("MQTT_TLS_SSL", "false")
            .parse()
            .context("MQTT_TLS_SSL must be bool")?,
        websocket_path: optional("MQTT_WEBSOCKET_PATH", "/mqtt").into(),
    })
}

/// Load Modbus settings from the first PLC in the mapping.
/// Returns `None` if there is no PLC device — weigher-only mode.
fn load_modbus_settings_opt(mapping: &DeviceMapping) -> Result<Option<ModbusSettings>> {
    let plc = match mapping
        .devices
        .iter()
        .find(|d| d.device_type == DeviceType::Master)
    {
        Some(p) => p,
        None => return Ok(None),
    };
    let unit_id = plc.connection.unit_id();
    let poll_interval_ms = mapping.poll_interval_ms.unwrap_or(1000);
    let (transport, rtu_timings) = device_conn_to_transport(&plc.connection)
        .context("convert DeviceConnection to Transport")?;
    Ok(Some(ModbusSettings {
        transport,
        unit_id,
        poll_interval: std::time::Duration::from_millis(poll_interval_ms),
        rtu_timings,
    }))
}

/// Convert `DeviceConnection` to `Transport` + `RtuTimings`.
/// The RTU timings use a 500ms/10ms default because
/// `DeviceConnection::RtuSerial` does not store timeout fields (those fields
/// lived in the old SourceTransport).
pub(crate) fn device_conn_to_transport(
    conn: &DeviceConnection,
) -> Result<(Transport, crate::modbus_actor::RtuTimings)> {
    use std::time::Duration;
    let default_timings = crate::modbus_actor::RtuTimings {
        request_timeout: Duration::from_millis(500),
        inter_frame_delay: Duration::from_millis(10),
    };
    match conn {
        DeviceConnection::Tcp { host, port, .. } => Ok((
            Transport::Tcp {
                host: host.clone(),
                port: *port,
            },
            default_timings,
        )),
        DeviceConnection::RtuSerial {
            path,
            baud,
            parity,
            stop_bits,
            data_bits,
            ..
        } => Ok((
            Transport::RtuSerial {
                path: path.clone(),
                baud: *baud,
                parity: parse_parity(parity)
                    .with_context(|| format!("connection.parity invalid: '{parity}'"))?,
                stop_bits: parse_stop_bits_u8(*stop_bits)
                    .with_context(|| format!("connection.stop_bits invalid: {stop_bits}"))?,
                data_bits: parse_data_bits_u8(*data_bits)
                    .with_context(|| format!("connection.data_bits invalid: {data_bits}"))?,
            },
            default_timings,
        )),
        DeviceConnection::SerialAscii { .. } => bail!(
            "device_conn_to_transport called for SerialAscii — \
             a weigher does not use a Modbus transport"
        ),
    }
}
/// Strip an inline comment from an env value. systemd `EnvironmentFile=` does
/// not strip inline `#` (unlike dotenvy). "none  # comment" → "none".
fn strip_env_comment(s: &str) -> &str {
    s.split('#').next().unwrap_or("").trim()
}

fn parse_parity(s: &str) -> Result<SerialParity> {
    match strip_env_comment(s).to_ascii_lowercase().as_str() {
        "none" | "n" => Ok(SerialParity::None),
        "even" | "e" => Ok(SerialParity::Even),
        "odd" | "o" => Ok(SerialParity::Odd),
        other => bail!("parity '{other}' is not recognized — use none/even/odd"),
    }
}

fn parse_stop_bits_u8(v: u8) -> Result<SerialStopBits> {
    match v {
        1 => Ok(SerialStopBits::One),
        2 => Ok(SerialStopBits::Two),
        other => bail!("transport.stop_bits {other} invalid — use 1 or 2"),
    }
}

fn parse_data_bits_u8(v: u8) -> Result<SerialDataBits> {
    match v {
        7 => Ok(SerialDataBits::Seven),
        8 => Ok(SerialDataBits::Eight),
        other => bail!("transport.data_bits {other} invalid — use 7 or 8"),
    }
}

/// Load gate config. Not fallible: missing/empty `CACHE_URL` = disabled (deny
/// all). Does not validate the URL format here — leave that to
/// `redis::Client::open` in [`crate::control_gate::ControlGate::connect`] which
/// reports a specific error.
fn load_control_gate_settings() -> ControlGateSettings {
    let raw = optional("CACHE_URL", "");
    let url = if raw.trim().is_empty() {
        None
    } else {
        Some(raw)
    };
    ControlGateSettings {
        url,
        hash_key: optional("CONTROL_GATE_KEY", crate::control_gate::DEFAULT_GATE_KEY),
    }
}

/// Load the mapping from a file. `DeviceMapping` format (a `devices` array), as
/// in `example-mapping.json`. See `DeviceMapping` in `shared/src/mapping.rs`.
fn load_mapping_file(path: impl AsRef<Path>) -> Result<DeviceMapping> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read mapping file: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| {
        format!(
            "parse mapping JSON (with a 'devices' array): {}",
            path.display()
        )
    })
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

// ===========================================================================
// Tests — pure parsing helpers. Env-driven loaders (load_modbus_settings,
// load_mqtt_settings) are not tested here because `env::var` is process-global
// and cargo tests run concurrently → races between tests. Smoke-tested manually.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_parity_accepts_long_and_short_forms() {
        assert_eq!(parse_parity("none").unwrap(), SerialParity::None);
        assert_eq!(parse_parity("n").unwrap(), SerialParity::None);
        assert_eq!(parse_parity("even").unwrap(), SerialParity::Even);
        assert_eq!(parse_parity("E").unwrap(), SerialParity::Even);
        assert_eq!(parse_parity("odd").unwrap(), SerialParity::Odd);
    }

    #[test]
    fn parse_parity_rejects_garbage() {
        assert!(parse_parity("mark").is_err());
        assert!(parse_parity("").is_err());
        assert!(parse_parity("1").is_err());
    }

    #[test]
    fn transport_display_renders_each_variant() {
        // Sanity: not testing a specific format, just making sure it doesn't
        // panic and each variant renders differently (catches the "all variants
        // render the same" bug).
        let tcp = Transport::Tcp {
            host: "192.168.1.20".into(),
            port: 502,
        };
        let rtu_tcp = Transport::RtuOverTcp {
            host: "192.168.1.30".into(),
            port: 4001,
        };
        let rtu_ser = Transport::RtuSerial {
            path: "/dev/ttyUSB0".into(),
            baud: 9600,
            parity: SerialParity::None,
            stop_bits: SerialStopBits::One,
            data_bits: SerialDataBits::Eight,
        };
        assert!(tcp.display().contains("tcp://"));
        assert!(rtu_tcp.display().contains("rtu-over-tcp://"));
        assert!(rtu_ser.display().contains("/dev/ttyUSB0"));
        assert!(rtu_ser.display().contains("9600"));
        // Make sure the three variants render differently
        assert_ne!(tcp.display(), rtu_tcp.display());
        assert_ne!(rtu_tcp.display(), rtu_ser.display());
    }
}
