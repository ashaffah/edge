//! Control + Set subscriber: receive an MQTT publish → write Modbus to the PLC
//! or a source device.
//!
//! ## Strategy (mixed-mode write, multi-device)
//!
//! - Scope: `write_single_coil`/`write_single_register` (gate-protected) AND
//!   `write_multiple_registers` (no gate) from every device in the mapping.
//! - Topic patterns subscribed (per ConnAck):
//!   - `{base}/{machine}/control/#`                        ← primary PLC control
//!   - `{base}/{machine}/set/#`                            ← primary PLC set
//!   - `{base}/{device.location}/{device.name}/control/#`  ← per device (additional PLC + slave)
//!   - `{base}/{device.location}/{device.name}/set/#`      ← per device
//! - Write semantics (determined directly from the `kind` binding):
//!   - `write_single_register` → FC6, write 1 register with an on/off value.
//!   - `write_single_coil` → FC5, write 1 boolean bit.
//!   - `write_multiple_registers` + Float → FC16, encode f32 → 2 registers.
//!   - `write_multiple_registers` + Integer → FC16, encode i32 → 2 registers.
//! - **Control gate**: `write_single_coil` and `write_single_register` must go
//!   through the gate. Check `HGETALL control` in Valkey — the AND of two flags
//!   (`global` + per-machine). `write_multiple_registers` (set/setpoint) bypasses
//!   the gate — it's a data write, not an actuator.
//! - **Multi-device writer** (keyed by `device_id`):
//!   - Primary PLC: writer factory from main.rs (connection from the first PLC
//!     in the mapping).
//!   - Additional PLC: writer factory from each one's connection block,
//!     independent. Supports split-port (port 502 read, 503 write) and
//!     multi-master (different PLCs).
//!   - Slave: writer factory from its connection block.
//!   - Routing uses `device_id` (not `unit_id`) so two PLCs with the same
//!     unit_id but different ports are dispatched to the correct connection.
//!   - Lazy connect on demand, drop + reconnect on error.
//! - Error handling:
//!   - Modbus connect failed → log + skip message, reconnect on the next message
//!   - Modbus write failed → log + drop this device_id's writer, reconnect next
//!   - Unknown topic → log debug, skip
//!   - Invalid payload → log warn, skip
//!   - Gate deny → log info, skip (by design)
//!   - Channel closed → exit loop

use crate::shared::{ByteOrder, DataType, DeviceMapping, ModbusBinding, TopicIndex};
use anyhow::{Context as _, Result, anyhow};
use rumqttc::{AsyncClient, Publish, QoS};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::control_gate::ControlGate;
use crate::modbus_client::{WriteConn, WriterFactory};
use crate::settings::Settings;

/// Minimum pause after a write failure before reconnecting to the same unit_id.
/// Gives the PLC time to close the old connection before a new one opens,
/// preventing EINPROGRESS on the first write after reconnect.
const CONNECT_SETTLE: Duration = Duration::from_millis(300);

/// The write action chosen from the binding + category. 4 variants:
/// - `Register`: control on/off devices (CONTROL grid). Write a boolean u16 value.
/// - `Coil`: momentary control (hydraulic up/down). Write bool.
/// - `SetFloat`: set_* float (setpoint). Parse f32 → encode 2 registers.
/// - `SetInteger`: set_* integer (setpoint). Parse i64 → cast i32 → encode 2 registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAction {
    /// Write a single holding register with a value based on the boolean payload.
    Register {
        address: u16,
        on_value: u16,
        off_value: u16,
    },
    /// Write a single coil. `true` payload → coil ON, `false` → coil OFF.
    Coil { address: u16 },
    /// Set float: parse the payload as f32, encode via byte_order → 2 registers.
    SetFloat { address: u16, byte_order: ByteOrder },
    /// Set integer: parse the payload i64, cast i32, bit-cast u32, encode via
    /// byte_order → 2 registers.
    SetInteger { address: u16, byte_order: ByteOrder },
}

/// Per-topic dispatch info. The key in `ControlSubscriberConfig.entries` =
/// the full MQTT topic (e.g. `acme/site/area1/machine_a/control/lamp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEntry {
    /// Param key (e.g. `control_lamp`, `set_speed`) for logging.
    pub key: String,
    /// The write action chosen per binding + data type.
    pub action: WriteAction,
    /// Target device ID — the key for looking up the writer factory and writer
    /// cache. Primary PLC = the device_id of the first PLC in the mapping;
    /// slave/additional PLC = its own device_id. Uses device_id (not unit_id) so
    /// two PLCs on different ports but with the same unit_id are dispatched to
    /// the correct connection.
    pub device_id: String,
    /// True if dispatch must go through the control_gate (= write_single_coil /
    /// write_single_register). False for write_multiple_registers (set/setpoint)
    /// — no gate by design.
    pub requires_gate: bool,
}

/// Runtime config for the control + set subscriber. Derived once at startup,
/// immutable for the life of the process.
#[derive(Clone)]
pub struct ControlSubscriberConfig {
    /// List of MQTT subscribe patterns. Subscribe to all of these after each
    /// ConnAck. MQTT wildcards `+`/`#` — matched pattern level-by-level.
    pub subscribe_patterns: Vec<String>,
    /// Writer factory per device_id. The primary PLC is always present;
    /// additional PLCs and slaves are built from each one's connection block.
    /// Key = device_id (not unit_id) so two devices with the same unit_id but
    /// different ports get the correct factory.
    pub writer_factories: HashMap<String, WriterFactory>,
    /// O(1) lookup from the full MQTT topic to the entry. Only entries with a
    /// writable binding are included; the rest are skipped.
    pub entries: HashMap<String, ControlEntry>,
}

impl ControlSubscriberConfig {
    pub async fn from_settings(
        settings: &Settings,
        plc_writer_factory: WriterFactory,
        actor_cache: &crate::modbus_client::ActorCache,
    ) -> Result<Self> {
        settings.modbus.as_ref().context(
            "ControlSubscriberConfig::from_settings called without a PLC in the mapping",
        )?;
        let idx = TopicIndex::from_device_mapping(&settings.base_topic, &settings.mapping);
        let entries = build_entries_map(&idx, &settings.mapping);

        // Writer factory per device_id.
        // Primary PLC: use the factory already built in main (plc_writer_factory).
        // Additional PLC + slave: built from each one's connection block.
        let mut writer_factories: HashMap<String, WriterFactory> = HashMap::new();
        let mut primary_plc_registered = false;
        for device in &settings.mapping.devices {
            use crate::shared::DeviceType;
            if device.device_type == DeviceType::Weigher {
                continue;
            }
            if device.device_type == DeviceType::Master && !primary_plc_registered {
                // Primary PLC — use the pre-built factory from main.rs
                writer_factories.insert(device.device_id.clone(), plc_writer_factory.clone());
                primary_plc_registered = true;
                continue;
            }
            // Additional PLC or slave — build the factory from the connection block.
            let factory =
                crate::modbus_client::build_source_writer(&device.connection, actor_cache)
                    .await
                    .with_context(|| format!("build writer for device '{}'", device.device_id))?;
            writer_factories
                .entry(device.device_id.clone())
                .or_insert(factory);
        }

        let subscribe_patterns = build_subscribe_patterns(
            &settings.base_topic,
            &settings.machine_id,
            &settings.mapping,
        );

        Ok(Self {
            subscribe_patterns,
            writer_factories,
            entries,
        })
    }
}

/// Assemble the list of subscribe patterns: primary PLC control/# + set/#, plus
/// one pair per other device (additional PLCs + slaves), using their own
/// location+name.
///
/// Pure function (`base` + `machine` + `mapping`, not `&Settings`) so it can be
/// unit-tested without env — like [`build_entries_map`].
fn build_subscribe_patterns(base: &str, machine: &str, mapping: &DeviceMapping) -> Vec<String> {
    let mut patterns = vec![
        format!("{base}/{machine}/control/#"),
        format!("{base}/{machine}/set/#"),
    ];
    // The primary PLC (first Master = machine_id) is already covered by the
    // machine patterns above. Every other device — additional PLCs and slaves —
    // has its own location/name and needs its own patterns. Skip only the first
    // Master; consistent with `primary_plc_registered` in `from_settings`.
    let mut primary_plc_skipped = false;
    for device in &mapping.devices {
        use crate::shared::DeviceType;
        if device.device_type == DeviceType::Master && !primary_plc_skipped {
            primary_plc_skipped = true;
            continue;
        }
        patterns.push(format!(
            "{base}/{}/{}/control/#",
            device.location, device.name
        ));
        patterns.push(format!("{base}/{}/{}/set/#", device.location, device.name));
    }
    patterns
}

/// Build the per-topic dispatch map from the TopicIndex. A pure function for
/// testability — no Settings/env needed.
///
/// Dispatch is determined directly from the `kind` binding (strict FC):
/// - `write_single_coil`        → `WriteAction::Coil`, requires_gate=true (FC5)
/// - `write_single_register`    → `WriteAction::Register`, requires_gate=true (FC6)
/// - `write_multiple_registers` + Float   → `WriteAction::SetFloat`, requires_gate=false (FC16)
/// - `write_multiple_registers` + Integer → `WriteAction::SetInteger`, requires_gate=false (FC16)
/// - Read variants + null modbus → skip (monitoring is handled by telemetry.rs)
///
/// Routing to the writer factory uses `device_id` (not unit_id) so two PLCs on
/// different ports but with the same unit_id get the correct connection.
fn build_entries_map(idx: &TopicIndex, mapping: &DeviceMapping) -> HashMap<String, ControlEntry> {
    // The primary PLC's device_id (device=None in TopicEntry).
    let primary_plc_device_id: String = mapping
        .devices
        .iter()
        .find(|d| d.device_type == crate::shared::DeviceType::Master)
        .map(|d| d.device_id.clone())
        .unwrap_or_default();

    let mut entries: HashMap<String, ControlEntry> = HashMap::new();
    for e in &idx.entries {
        // Resolve device_id based on the device origin.
        let device_id = match &e.device {
            None => primary_plc_device_id.clone(),
            Some(id) => {
                // Defensive: make sure the device_id exists in the mapping.
                if mapping.devices.iter().all(|d| &d.device_id != id) {
                    continue;
                }
                id.clone()
            }
        };

        let (action, requires_gate) = match (e.modbus.as_ref(), e.data_type) {
            // --- FC5: Write Single Coil (boolean control, requires gate) ---
            (Some(ModbusBinding::WriteSingleCoil { address }), _) => {
                (WriteAction::Coil { address: *address }, true)
            }
            // --- FC6: Write Single Register (on/off control, requires gate) ---
            (
                Some(ModbusBinding::WriteSingleRegister {
                    address,
                    on_value,
                    off_value,
                }),
                _,
            ) => (
                WriteAction::Register {
                    address: *address,
                    on_value: *on_value,
                    off_value: off_value.unwrap_or(0),
                },
                true,
            ),
            // --- FC16: Write Multiple Registers float (setpoint, no gate) ---
            (
                Some(ModbusBinding::WriteMultipleRegisters {
                    address,
                    byte_order,
                }),
                DataType::Float,
            ) => (
                WriteAction::SetFloat {
                    address: *address,
                    byte_order: *byte_order,
                },
                false,
            ),
            // --- FC16: Write Multiple Registers integer (setpoint, no gate) ---
            (
                Some(ModbusBinding::WriteMultipleRegisters {
                    address,
                    byte_order,
                }),
                DataType::Integer,
            ) => (
                WriteAction::SetInteger {
                    address: *address,
                    byte_order: *byte_order,
                },
                false,
            ),
            // Everything else: skip (monitoring, no binding, no on_value, etc.)
            _ => continue,
        };

        entries.insert(
            e.topic.clone(),
            ControlEntry {
                key: e.key.clone(),
                action,
                device_id,
                requires_gate,
            },
        );
    }
    entries
}

/// Parse a boolean payload (for control register/coil). Accepts `"1"`, `"0"`,
/// `"true"`, `"false"` (case-insensitive, whitespace trimmed). The control
/// publisher is expected to send plain text `"1"` or `"0"`.
pub fn parse_bool_payload(payload: &[u8]) -> Result<bool> {
    let s = std::str::from_utf8(payload).map_err(|_| anyhow!("payload not utf-8"))?;
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty payload"));
    }
    match s.to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(anyhow!("invalid bool payload: {other:?}")),
    }
}

/// Parse a float payload for set_* writes. Accepts any f32-parseable string
/// (trimmed). Returns `Err` if the utf-8 is invalid or parsing fails.
fn parse_float_payload(payload: &[u8]) -> Result<f32> {
    let s = std::str::from_utf8(payload).map_err(|_| anyhow!("payload not utf-8"))?;
    s.trim()
        .parse::<f32>()
        .with_context(|| format!("invalid f32 payload {:?}", s.trim()))
}

/// Parse an integer payload for set_* writes. Accepts an i64-range string. The
/// caller (dispatch_action) casts to i32 when encoding.
fn parse_integer_payload(payload: &[u8]) -> Result<i64> {
    let s = std::str::from_utf8(payload).map_err(|_| anyhow!("payload not utf-8"))?;
    s.trim()
        .parse::<i64>()
        .with_context(|| format!("invalid i64 payload {:?}", s.trim()))
}

/// Handle 1 incoming Publish. Look up the entry, gate-check (if control), ensure
/// the writer exists (lazy connect), dispatch.
///
/// Returns `Ok` if handled (write succeeded, skipped with a log, or gate deny).
/// Returns `Err` if the Modbus write failed — the caller drops the writer for
/// this device_id so it reconnects on the next message.
async fn handle_publish(
    p: &Publish,
    config: &ControlSubscriberConfig,
    writers: &mut HashMap<String, Box<dyn WriteConn>>,
    gate: &mut ControlGate,
    last_write_fail: &mut HashMap<String, Instant>,
) -> Result<()> {
    let Some(entry) = config.entries.get(&p.topic) else {
        debug!(
            topic = %p.topic,
            "control: unknown topic (no binding or not control/set writable), skip"
        );
        return Ok(());
    };

    // Gate check ONLY for write_single_coil / write_single_register.
    if entry.requires_gate && !gate.is_granted().await {
        info!(
            key = %entry.key,
            topic = %p.topic,
            device_id = %entry.device_id,
            machine = %gate.machine_field(),
            "control: gate DENY, skip (grant: `redis-cli HSET control global 1 && redis-cli HSET control {} 1`)",
            gate.machine_field()
        );
        return Ok(());
    }

    // Lazy connect the writer for this device_id.
    if let std::collections::hash_map::Entry::Vacant(e) = writers.entry(entry.device_id.clone()) {
        // Settle delay after a previous write failure (prevents EINPROGRESS).
        if let Some(&failed_at) = last_write_fail.get(&entry.device_id) {
            let elapsed = failed_at.elapsed();
            if elapsed < CONNECT_SETTLE {
                tokio::time::sleep(CONNECT_SETTLE - elapsed).await;
            }
            last_write_fail.remove(&entry.device_id);
        }
        let factory = config
            .writer_factories
            .get(&entry.device_id)
            .ok_or_else(|| anyhow!("no writer factory for device_id {}", entry.device_id))?;
        match factory.connect().await {
            Ok(w) => {
                info!(device_id = %entry.device_id, "modbus writer connected");
                e.insert(w);
            }
            Err(e) => {
                warn!(
                    device_id = %entry.device_id,
                    "modbus writer connect failed: {e:#}, skip this message"
                );
                return Ok(());
            }
        }
    }
    let writer = writers.get_mut(&entry.device_id).unwrap();

    let dispatch_result = dispatch_action(entry, &p.payload, writer).await;
    if dispatch_result.is_err() {
        // Drop the writer — other devices are unaffected (independent per device_id).
        writers.remove(&entry.device_id);
        last_write_fail.insert(entry.device_id.clone(), Instant::now());
    }
    dispatch_result
}

/// Dispatch one entry per its action. The caller (handle_publish) has already
/// verified the entry exists + done the gate check.
///
/// Return semantics (important for the writer lifecycle in the caller):
/// - `Ok(())` → write succeeded OR payload invalid (writer still valid, skip message).
/// - `Err`    → Modbus write failed → the caller must drop the writer to reconnect.
///
/// A parse error is NOT propagated as `Err`: a corrupt payload is not a Modbus
/// connection error. Propagating a parse error would drop the writer and
/// reconnect needlessly on every invalid payload.
async fn dispatch_action(
    entry: &ControlEntry,
    payload: &[u8],
    writer: &mut Box<dyn WriteConn>,
) -> Result<()> {
    match &entry.action {
        WriteAction::Register {
            address,
            on_value,
            off_value,
        } => {
            let payload_bool = match parse_bool_payload(payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!(key = %entry.key, "control: invalid payload, skip write: {e:#}");
                    return Ok(());
                }
            };
            let value = if payload_bool { *on_value } else { *off_value };
            writer
                .write_single_register(*address, value)
                .await
                .with_context(|| {
                    format!(
                        "write register for {} (device={}, addr={}, value={})",
                        entry.key, entry.device_id, address, value
                    )
                })?;
            info!(
                key = %entry.key,
                kind = "register",
                device_id = %entry.device_id,
                address = *address,
                payload = payload_bool,
                value,
                "control written"
            );
        }
        WriteAction::Coil { address } => {
            let payload_bool = match parse_bool_payload(payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!(key = %entry.key, "control: invalid payload, skip write: {e:#}");
                    return Ok(());
                }
            };
            writer
                .write_single_coil(*address, payload_bool)
                .await
                .with_context(|| {
                    format!(
                        "write coil for {} (device={}, addr={})",
                        entry.key, entry.device_id, address
                    )
                })?;
            info!(
                key = %entry.key,
                kind = "coil",
                device_id = %entry.device_id,
                address = *address,
                value = payload_bool,
                "control written"
            );
        }
        WriteAction::SetFloat {
            address,
            byte_order,
        } => {
            let value = match parse_float_payload(payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!(key = %entry.key, "set: invalid payload, skip write: {e:#}");
                    return Ok(());
                }
            };
            let regs = byte_order.encode_f32(value);
            writer
                .write_multiple_registers(*address, regs.to_vec())
                .await
                .with_context(|| {
                    format!(
                        "write set_float for {} (device={}, addr={}, value={})",
                        entry.key, entry.device_id, address, value
                    )
                })?;
            info!(
                key = %entry.key,
                kind = "set_float",
                device_id = %entry.device_id,
                address = *address,
                value,
                "set written"
            );
        }
        WriteAction::SetInteger {
            address,
            byte_order,
        } => {
            let value = match parse_integer_payload(payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!(key = %entry.key, "set: invalid payload, skip write: {e:#}");
                    return Ok(());
                }
            };
            // i64 → i32 truncate (set values fit in i32 for all params today;
            // out-of-range = caller error). Bit-cast i32 → u32 preserves two's
            // complement so negative values decode correctly on the PLC. Cast
            // `as u32` here is equivalent to `u32::from_ne_bytes(v32.to_ne_bytes())`.
            let v32 = value as i32;
            let regs = byte_order.encode_u32(v32 as u32);
            writer
                .write_multiple_registers(*address, regs.to_vec())
                .await
                .with_context(|| {
                    format!(
                        "write set_integer for {} (device={}, addr={}, value={})",
                        entry.key, entry.device_id, address, value
                    )
                })?;
            info!(
                key = %entry.key,
                kind = "set_integer",
                device_id = %entry.device_id,
                address = *address,
                value,
                "set written"
            );
        }
    }
    Ok(())
}

/// Main control + set subscriber loop. Loops: select between ConnAck →
/// subscribe all patterns + inbox → dispatch to the appropriate writer.
///
/// Writers are cached per device_id. Lazy connect on the first message for that
/// device. Dropped on a write error → reconnect on the next message.
///
/// `connack_rx`: a signal channel from `mqtt::drive_eventloop` on every ConnAck
/// (initial connect + every reconnect). rumqttc defaults to
/// `clean_session=true` — the broker drops subscriptions when the client
/// disconnects, so we must re-subscribe after reconnect. Subscribing is also
/// triggered by the first ConnAck (no separate initial subscribe needed).
///
/// `gate`: the Valkey/Redis authorization gate. Checked on every Control-category
/// message (set_* bypasses the gate). Owned by-value so its inner state
/// (ConnectionManager) can be mutated without a Mutex/Arc.
pub async fn run(
    client: AsyncClient,
    config: ControlSubscriberConfig,
    mut inbox: mpsc::Receiver<Publish>,
    mut connack_rx: mpsc::Receiver<()>,
    mut gate: ControlGate,
) {
    info!(
        patterns = ?config.subscribe_patterns,
        bindings = config.entries.len(),
        devices = config.writer_factories.len(),
        "control subscriber starting"
    );

    // Writer cache per device_id. Lazy connect on the first message for that
    // device. Dropped on error → next message reconnects.
    let mut writers: HashMap<String, Box<dyn WriteConn>> = HashMap::new();
    // Last write-failure time per device_id, for the settle delay before reconnect.
    let mut last_write_fail: HashMap<String, Instant> = HashMap::new();

    loop {
        tokio::select! {
            // biased: prioritize ConnAck so if re-subscribe and dispatch are
            // both ready, subscribe first — minimizing the window in which a
            // control command could be missed after a reconnect.
            biased;
            connack = connack_rx.recv() => {
                match connack {
                    Some(()) => {
                        for pattern in &config.subscribe_patterns {
                            match client.subscribe(pattern, QoS::AtLeastOnce).await {
                                Ok(()) => info!(
                                    pattern = %pattern,
                                    "control (re-)subscribed after ConnAck"
                                ),
                                Err(e) => warn!(
                                    pattern = %pattern,
                                    "control subscribe failed: {e:#}"
                                ),
                            }
                        }
                    }
                    None => {
                        info!("connack channel closed, exiting");
                        return;
                    }
                }
            }
            msg = inbox.recv() => {
                match msg {
                    Some(p) => {
                        if let Err(e) = handle_publish(&p, &config, &mut writers, &mut gate, &mut last_write_fail).await {
                            warn!(
                                topic = %p.topic,
                                "control dispatch failed: {e:#} (device writer dropped, will reconnect)"
                            );
                            // No explicit backoff here — the next message
                            // targeting the same device_id reconnects via the
                            // lazy path in handle_publish. If Modbus stays down,
                            // the lazy connect keeps failing but doesn't block
                            // the loop.
                        }
                    }
                    None => {
                        // Inbox closed = eventloop probably stopped = exit.
                        info!("control inbox closed, exiting");
                        return;
                    }
                }
            }
        }
    }
}

// ===========================================================================
// Tests — pure functions only (parse + filter + factory wiring).
// Loop manual-smoke.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_mapping_json() -> &'static str {
        // Multi-device subset (devices array):
        // - control_lamp: holding+on/off=1/3 (on/off device)        → Register
        // - control_valve_up: coil at addr 4 (momentary)          → Coil
        // - control_pump: no binding                              → skipped
        // - set_speed: holding float (data-only binding)         → SetFloat
        // - set_time: holding integer                            → SetInteger
        // - chiller slave: monitoring + set + control               → 2 writable
        r#"{
            "base_topic": "acme/site",
            "poll_interval_ms": 1000,
            "devices": [
                {
                    "device_type": "master",
                    "device_id": "plc1",
                    "location": "area1",
                    "name": "machine_a",
                    "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                    "monitoring": [
                        { "key": "current_motor", "label": "Motor Current", "topic": "current/motor",
                          "type": "float", "unit": "A",
                          "modbus": { "kind": "read_holding_registers", "address": 10 } }
                    ],
                    "set": [
                        { "key": "set_speed", "label": "Speed Setpoint", "topic": "set/speed",
                          "type": "float", "unit": "rpm",
                          "modbus": { "kind": "write_multiple_registers", "address": 22 } },
                        { "key": "set_time", "label": "Time Setpoint", "topic": "set/time",
                          "type": "integer", "unit": "s",
                          "modbus": { "kind": "write_multiple_registers", "address": 32 } }
                    ],
                    "control": [
                        { "key": "control_lamp", "label": "Lamp On/Off", "topic": "control/lamp",
                          "type": "boolean",
                          "modbus": { "kind": "write_single_register", "address": 10, "on_value": 1, "off_value": 3 } },
                        { "key": "control_valve_up", "label": "Valve Open", "topic": "control/valve/up",
                          "type": "boolean",
                          "modbus": { "kind": "write_single_coil", "address": 4 } },
                        { "key": "control_pump", "label": "Pump On/Off", "topic": "control/pump",
                          "type": "boolean" }
                    ]
                },
                {
                    "device_type": "slave",
                    "device_id": "slave1",
                    "location": "area1",
                    "name": "chiller",
                    "connection": { "type": "rtu_serial", "unit_id": 2, "path": "/dev/ttyUSB0",
                                    "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
                    "monitoring": [
                        { "key": "temp_out_chiller", "label": "Chiller Outlet Temperature",
                          "topic": "temp/out", "type": "float", "unit": "C",
                          "modbus": { "kind": "read_holding_registers", "address": 0 } }
                    ],
                    "set": [
                        { "key": "set_temp_chiller", "label": "Chiller Temp Setpoint",
                          "topic": "set/temp", "type": "float", "unit": "C",
                          "modbus": { "kind": "write_multiple_registers", "address": 20 } }
                    ],
                    "control": [
                        { "key": "control_chiller_on", "label": "Chiller ON/OFF",
                          "topic": "control/on", "type": "boolean",
                          "modbus": { "kind": "write_single_register", "address": 30, "on_value": 1, "off_value": 3 } }
                    ]
                }
            ]
        }"#
    }

    fn build_test_mapping() -> DeviceMapping {
        serde_json::from_str(test_mapping_json()).expect("parse DeviceMapping")
    }

    fn build_test_index(m: &DeviceMapping) -> TopicIndex {
        TopicIndex::from_device_mapping("acme/site", m)
    }

    // ---------- parse_bool_payload ----------

    #[test]
    fn parse_payload_accepts_one_zero_true_false() {
        assert!(parse_bool_payload(b"1").unwrap());
        assert!(!parse_bool_payload(b"0").unwrap());
        assert!(parse_bool_payload(b"true").unwrap());
        assert!(!parse_bool_payload(b"false").unwrap());
    }

    #[test]
    fn parse_payload_case_insensitive() {
        assert!(parse_bool_payload(b"TRUE").unwrap());
        assert!(!parse_bool_payload(b"False").unwrap());
        assert!(parse_bool_payload(b"TrUe").unwrap());
    }

    #[test]
    fn parse_payload_trims_whitespace() {
        assert!(parse_bool_payload(b"  1  ").unwrap());
        assert!(!parse_bool_payload(b"\n0\t").unwrap());
    }

    #[test]
    fn parse_payload_rejects_garbage() {
        assert!(parse_bool_payload(b"abc").is_err());
        assert!(parse_bool_payload(b"2").is_err());
        assert!(parse_bool_payload(b"yes").is_err()); // strict: only 1/0/true/false
        assert!(parse_bool_payload(b"").is_err());
        assert!(parse_bool_payload(b"   ").is_err());
    }

    #[test]
    fn parse_payload_rejects_non_utf8() {
        // 0xFF is not a valid UTF-8 start byte
        assert!(parse_bool_payload(&[0xFF, 0xFE]).is_err());
    }

    // ---------- parse_float_payload + parse_integer_payload ----------

    #[test]
    fn parse_float_accepts_decimal_and_scientific() {
        assert_eq!(parse_float_payload(b"23.5").unwrap(), 23.5f32);
        assert_eq!(parse_float_payload(b"-12.5").unwrap(), -12.5f32);
        assert_eq!(parse_float_payload(b"  1.5e3  ").unwrap(), 1500.0f32);
    }

    #[test]
    fn parse_float_rejects_garbage() {
        assert!(parse_float_payload(b"abc").is_err());
        assert!(parse_float_payload(b"").is_err());
        assert!(parse_float_payload(b"12.5x").is_err());
    }

    #[test]
    fn parse_integer_accepts_negative_and_positive() {
        assert_eq!(parse_integer_payload(b"42").unwrap(), 42i64);
        assert_eq!(parse_integer_payload(b"-7").unwrap(), -7i64);
        assert_eq!(parse_integer_payload(b"  0  ").unwrap(), 0i64);
    }

    #[test]
    fn parse_integer_rejects_float() {
        // Strict: an integer payload does not accept "3.0" — the publisher is
        // expected to have typed-cast before publishing, so the payload is always
        // an integer literal.
        assert!(parse_integer_payload(b"3.0").is_err());
        assert!(parse_integer_payload(b"abc").is_err());
    }

    // ---------- build_entries_map ----------

    /// PLC + source: control register/coil + set float/integer, all entries with
    /// the correct device_id (PLC = plc1, chiller = slave1). Monitoring +
    /// no-binding are skipped.
    #[test]
    fn entries_includes_control_coil_register_and_set_for_plc_and_source() {
        let m = build_test_mapping();
        let idx = build_test_index(&m);
        let entries = build_entries_map(&idx, &m);

        // Expected entries (6):
        // PLC:    control_lamp, control_valve_up, set_speed, set_time
        // Source: control_chiller_on, set_temp_chiller
        assert_eq!(
            entries.len(),
            6,
            "should include 4 PLC + 2 chiller writable entries"
        );

        // --- PLC entries (device_id = "plc1") ---
        let lamp = entries
            .get("acme/site/area1/machine_a/control/lamp")
            .expect("control_lamp missing");
        assert_eq!(
            lamp.device_id, "plc1",
            "PLC entries use primary PLC device_id"
        );
        assert!(lamp.requires_gate, "control = gate-protected");
        assert_eq!(
            lamp.action,
            WriteAction::Register {
                address: 10,
                on_value: 1,
                off_value: 3,
            }
        );

        let valve = entries
            .get("acme/site/area1/machine_a/control/valve/up")
            .expect("control_valve_up missing");
        assert_eq!(valve.device_id, "plc1");
        assert!(valve.requires_gate);
        assert_eq!(valve.action, WriteAction::Coil { address: 4 });

        let speed = entries
            .get("acme/site/area1/machine_a/set/speed")
            .expect("set_speed missing");
        assert_eq!(speed.device_id, "plc1");
        assert!(!speed.requires_gate, "set_* bypass gate");
        assert!(matches!(
            speed.action,
            WriteAction::SetFloat { address: 22, .. }
        ));

        let time = entries
            .get("acme/site/area1/machine_a/set/time")
            .expect("set_time missing");
        assert_eq!(time.device_id, "plc1");
        assert!(!time.requires_gate);
        assert!(matches!(
            time.action,
            WriteAction::SetInteger { address: 32, .. }
        ));

        // --- Source entries (device_id = "slave1") ---
        let chiller_on = entries
            .get("acme/site/area1/chiller/control/on")
            .expect("control_chiller_on missing");
        assert_eq!(
            chiller_on.device_id, "slave1",
            "chiller uses slave device_id"
        );
        assert!(
            chiller_on.requires_gate,
            "control = gate-protected (source)"
        );
        assert_eq!(
            chiller_on.action,
            WriteAction::Register {
                address: 30,
                on_value: 1,
                off_value: 3,
            }
        );

        let chiller_temp = entries
            .get("acme/site/area1/chiller/set/temp")
            .expect("set_temp_chiller missing");
        assert_eq!(chiller_temp.device_id, "slave1");
        assert!(!chiller_temp.requires_gate, "set on source bypass gate");
        assert!(matches!(
            chiller_temp.action,
            WriteAction::SetFloat { address: 20, .. }
        ));

        // --- Excluded ---
        // current_motor: monitoring (handled by telemetry)
        assert!(!entries.contains_key("acme/site/area1/machine_a/current/motor"));
        // control_pump: no binding
        assert!(!entries.contains_key("acme/site/area1/machine_a/control/pump"));
        // chiller temp_out: monitoring (handled by telemetry source session)
        assert!(!entries.contains_key("acme/site/area1/chiller/temp/out"));
    }

    /// Defensive: a control_* with `kind: read_holding_registers` is skipped —
    /// that's a read-only / set_* placeholder, not a write target. Without an
    /// `on_value`, dispatch wouldn't know what value to write for payload=true.
    #[test]
    fn entries_excludes_read_binding_in_control_section() {
        use crate::shared::DeviceMapping;
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [{
                "device_type": "master", "device_id": "plc1", "location": "area1", "name": "machine_a",
                "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                "monitoring": [], "set": [],
                "control": [
                    { "key": "control_read_only", "label": "Read Only", "topic": "control/ro",
                      "type": "float",
                      "modbus": { "kind": "read_holding_registers", "address": 100 } }
                ]
            }]
        }"#;
        let m = serde_json::from_str::<DeviceMapping>(json).unwrap();
        let idx = TopicIndex::from_device_mapping("acme/site", &m);
        let entries = build_entries_map(&idx, &m);
        assert!(
            entries.is_empty(),
            "a read binding in the control section must be skipped (not a write variant)"
        );
    }

    /// `off_value` absent di JSON → default 0 di ControlEntry (safe release).
    #[test]
    fn entries_default_off_value_to_zero_when_absent() {
        use crate::shared::DeviceMapping;
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [{
                "device_type": "master", "device_id": "plc1", "location": "area1", "name": "machine_a",
                "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                "monitoring": [], "set": [],
                "control": [
                    { "key": "control_pulse", "label": "Pulse", "topic": "control/pulse",
                      "type": "boolean",
                      "modbus": { "kind": "write_single_register", "address": 50, "on_value": 1 } }
                ]
            }]
        }"#;
        let m = serde_json::from_str::<DeviceMapping>(json).unwrap();
        let idx = TopicIndex::from_device_mapping("acme/site", &m);
        let entries = build_entries_map(&idx, &m);
        let pulse = entries
            .get("acme/site/area1/machine_a/control/pulse")
            .expect("control_pulse should be included");
        assert_eq!(
            pulse.action,
            WriteAction::Register {
                address: 50,
                on_value: 1,
                off_value: 0,
            },
            "off_value must default to 0 when absent in JSON"
        );
    }

    // ---------- subscribe pattern building ----------

    /// The subscribe pattern list covers the PLC (control + set) + 1 pattern per
    /// source (control + set). Sources have their own location+name → the
    /// pattern is not nested under the machine.
    #[test]
    fn subscribe_patterns_cover_plc_and_each_source() {
        // PLC at area1/machine_a, chiller at area1/chiller (its own location+name).
        // Expected 4 patterns: 2 PLC + 2 chiller.
        let expected: Vec<String> = vec![
            "acme/site/area1/machine_a/control/#".into(),
            "acme/site/area1/machine_a/set/#".into(),
            "acme/site/area1/chiller/control/#".into(),
            "acme/site/area1/chiller/set/#".into(),
        ];
        assert_eq!(expected.len(), 4);
        assert!(expected.contains(&"acme/site/area1/machine_a/control/#".to_string()));
        assert!(expected.contains(&"acme/site/area1/machine_a/set/#".to_string()));
        assert!(expected.contains(&"acme/site/area1/chiller/control/#".to_string()));
        assert!(expected.contains(&"acme/site/area1/chiller/set/#".to_string()));
    }

    /// Regression: two PLCs on one edge-client (machine_a primary + machine_b
    /// second). The primary is covered by the machine patterns; the second PLC
    /// must get its own patterns, otherwise its control is never subscribed
    /// (multi-PLC bug).
    #[test]
    fn subscribe_patterns_include_every_plc_not_just_the_first() {
        use crate::shared::DeviceMapping;
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [
                {
                    "device_type": "master", "device_id": "plc1", "location": "area1", "name": "machine_a",
                    "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                    "monitoring": [], "set": [],
                    "control": [
                        { "key": "control_lamp", "label": "Lamp", "topic": "control/lamp",
                          "type": "boolean", "modbus": { "kind": "write_single_coil", "address": 5 } }
                    ]
                },
                {
                    "device_type": "master", "device_id": "plc2", "location": "area2", "name": "machine_b",
                    "connection": { "type": "tcp", "host": "192.168.10.11", "port": 502, "unit_id": 1 },
                    "monitoring": [], "set": [],
                    "control": [
                        { "key": "control_lamp", "label": "Lamp", "topic": "control/lamp",
                          "type": "boolean", "modbus": { "kind": "write_single_coil", "address": 5 } }
                    ]
                }
            ]
        }"#;
        let mapping = serde_json::from_str::<DeviceMapping>(json).unwrap();
        let patterns = build_subscribe_patterns("acme/site", "area1/machine_a", &mapping);

        // Primary (machine_a) via machine patterns.
        assert!(patterns.contains(&"acme/site/area1/machine_a/control/#".to_string()));
        assert!(patterns.contains(&"acme/site/area1/machine_a/set/#".to_string()));
        // Second PLC (machine_b) — the bug: this was missing.
        assert!(
            patterns.contains(&"acme/site/area2/machine_b/control/#".to_string()),
            "second PLC control topic must be subscribed"
        );
        assert!(patterns.contains(&"acme/site/area2/machine_b/set/#".to_string()));
        // Primary must not be subscribed twice (machine + device pattern).
        assert_eq!(
            patterns
                .iter()
                .filter(|p| p.as_str() == "acme/site/area1/machine_a/control/#")
                .count(),
            1,
            "primary PLC must not be double-subscribed"
        );
    }
}
