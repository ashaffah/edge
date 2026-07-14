//! MQTT parameter mapping and its lookup index.
//!
//! Pure domain types — no I/O here. The file loader lives in `settings.rs`
//! (because it loads together with env).

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashMap;

use super::modbus::ByteOrder;

/// One bridge rule: read a value from `read_from`, write it to `write_to`.
///
/// The bridge task runs every poll interval. If the PLC and the operator
/// dashboard both write to the same target, the PLC (via the bridge) always wins
/// on the next cycle (≤1 poll interval). The bridge must publish the written
/// value to MQTT after the write so the dashboard stays in sync.
///
/// The bridge task does **not** go through control_gate (automated write, not an
/// operator). control_gate is only for the MQTT-triggered control subscriber.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeRule {
    pub read_from: Endpoint,
    pub write_to: Endpoint,
    #[serde(default)]
    pub transform: TransformKind,
}

/// A reference to one parameter on one device.
/// `device = "plc"` → the `sources["plc"]` entry (main PLC). Any other name → a
/// key in `sources`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub device: String,
    pub key: String,
}

/// The transform applied when the bridge copies a value.
/// Currently only `passthrough` — extensible without a breaking change.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformKind {
    #[default]
    Passthrough,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Param {
    pub key: String,
    pub label: String,
    pub topic: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default)]
    pub unit: Option<String>,
    /// Modbus binding for edge-client.
    /// A parameter without `modbus` is not polled by edge-client.
    #[serde(default)]
    pub modbus: Option<ModbusBinding>,
}

// ---------------------------------------------------------------------------
// Modbus binding
// ---------------------------------------------------------------------------

/// Modbus binding per parameter — strictly one variant per Function Code.
/// `kind` in JSON directly states the FC used, without inferring it from
/// context (monitoring/set/control category).
///
/// Read variants (used in `monitoring[]`):
/// - `read_coils`             → FC1, bulk read boolean bits from address 0
/// - `read_discrete_inputs`   → FC2, targeted single-bit read (sensor/limit switch)
/// - `read_holding_registers` → FC3, bulk read float/integer from address 0
/// - `read_input_registers`   → FC4, targeted read (sparse address, e.g. chiller 1000)
///
/// Write variants (used in `control[]` and `set[]`):
/// - `write_single_coil`       → FC5, write 1 boolean bit to a coil
/// - `write_single_register`   → FC6, write 1 16-bit register (on/off via an integer value)
/// - `write_multiple_registers`→ FC16, write 2 registers (float32 or int32 setpoint)
///
/// Example JSON:
/// ```json
/// // FC3 monitoring float:
/// { "kind": "read_holding_registers", "address": 10, "byte_order": "big_little" }
///
/// // FC5 control coil:
/// { "kind": "write_single_coil", "address": 8212 }
///
/// // FC6 control register on/off:
/// { "kind": "write_single_register", "address": 6, "on_value": 1, "off_value": 3 }
///
/// // FC16 set float setpoint:
/// { "kind": "write_multiple_registers", "address": 200, "byte_order": "big_little" }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModbusBinding {
    /// FC1 — Read Coils. Bulk read boolean bits from address 0.
    ReadCoils { address: u16 },
    /// FC2 — Read Discrete Inputs. Targeted single-bit read, read-only.
    ReadDiscreteInputs { address: u16 },
    /// FC3 — Read Holding Registers. Bulk read 2 registers (f32/i32) from address 0.
    ReadHoldingRegisters {
        address: u16,
        #[serde(default)]
        byte_order: ByteOrder,
        #[serde(default)]
        scale: Option<f64>,
    },
    /// FC4 — Read Input Registers. Targeted read, address can be sparse.
    /// `scale`: optional multiplier (e.g. 0.1 for chiller raw×0.1=°C).
    ReadInputRegisters {
        address: u16,
        #[serde(default)]
        byte_order: ByteOrder,
        #[serde(default)]
        scale: Option<f64>,
    },
    /// FC5 — Write Single Coil. Write 1 boolean bit.
    WriteSingleCoil { address: u16 },
    /// FC6 — Write Single Register. Write 1 16-bit register.
    /// `on_value`: the value when payload=true. `off_value`: the value when
    /// payload=false (default 0 if absent).
    WriteSingleRegister {
        address: u16,
        on_value: u16,
        #[serde(default)]
        off_value: Option<u16>,
    },
    /// FC16 — Write Multiple Registers. Write 2 registers for a float32/int32 setpoint.
    WriteMultipleRegisters {
        address: u16,
        #[serde(default)]
        byte_order: ByteOrder,
    },
}

impl ModbusBinding {
    /// The starting register/coil/bit address.
    pub fn address(&self) -> u16 {
        match self {
            Self::ReadCoils { address }
            | Self::ReadDiscreteInputs { address }
            | Self::ReadHoldingRegisters { address, .. }
            | Self::ReadInputRegisters { address, .. }
            | Self::WriteSingleCoil { address }
            | Self::WriteSingleRegister { address, .. }
            | Self::WriteMultipleRegisters { address, .. } => *address,
        }
    }

    /// True if this binding reads holding registers (FC3).
    pub fn is_read_holding(&self) -> bool {
        matches!(self, Self::ReadHoldingRegisters { .. })
    }

    /// True if this binding reads coils (FC1).
    pub fn is_read_coil(&self) -> bool {
        matches!(self, Self::ReadCoils { .. })
    }

    /// True if this binding reads input registers (FC4).
    pub fn is_read_input_register(&self) -> bool {
        matches!(self, Self::ReadInputRegisters { .. })
    }

    /// True if this binding reads discrete inputs (FC2).
    pub fn is_read_discrete_input(&self) -> bool {
        matches!(self, Self::ReadDiscreteInputs { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Float,
    Integer,
    Boolean,
    /// Raw string passthrough — no conversion. Used for parameters whose value
    /// is not numeric, e.g. a scale unit ("kg", "g", "lb"). Cannot be used as
    /// set/control.
    String,
}

impl DataType {
    /// Parse a raw payload (string from MQTT) into a typed value.
    pub fn parse(&self, raw: &str) -> Result<TypedValue> {
        let raw = raw.trim();
        match self {
            Self::Float => Ok(TypedValue::Float(raw.parse().context("invalid float")?)),
            Self::Integer => Ok(TypedValue::Integer(raw.parse().context("invalid integer")?)),
            Self::Boolean => {
                let v = match raw.to_ascii_lowercase().as_str() {
                    "true" | "1" | "on" => true,
                    "false" | "0" | "off" => false,
                    other => bail!("invalid boolean payload: {other:?}"),
                };
                Ok(TypedValue::Boolean(v))
            }
            Self::String => Ok(TypedValue::String(raw.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Monitoring,
    Set,
    Control,
}

impl Category {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Monitoring => "monitoring",
            Self::Set => "set",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Clone)]
pub enum TypedValue {
    Float(f64),
    Integer(i64),
    Boolean(bool),
    String(std::string::String),
}

// ---------------------------------------------------------------------------
// Resolved entry + index
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TopicEntry {
    pub machine: String,
    /// `None` = main PLC (key `"plc"` in `sources`).
    /// `Some(name)` = another source device (key = device_id).
    pub device: Option<String>,
    pub category: Category,
    pub key: String,
    pub label: String,
    pub topic: String,
    pub data_type: DataType,
    pub unit: Option<String>,
    /// Modbus binding if present. `None` means the parameter is not polled by
    /// edge-client.
    pub modbus: Option<ModbusBinding>,
}

/// O(1) lookup from a full MQTT topic to an entry. Built once at startup from
/// the Mapping × the list of machines.
#[derive(Debug, Clone)]
pub struct TopicIndex {
    pub entries: Vec<TopicEntry>,
    by_topic: HashMap<String, usize>,
}

impl TopicIndex {
    /// Build the index for edge-client from a `DeviceMapping`.
    ///
    /// The **primary PLC** (first PLC in the array) gets `device = None` — used
    /// by the main bulk read loop in telemetry.
    /// **Additional PLCs** (second, etc.) get `device = Some(device_id)` — polled
    /// as independent source sessions, allowing a different TCP connection (e.g.
    /// a different port for read vs write on Siemens).
    /// Slaves get `device = Some(device_id)`.
    pub fn from_device_mapping(base_topic: &str, mapping: &DeviceMapping) -> Self {
        let mut entries = Vec::new();
        let mut primary_plc_seen = false;
        for device in &mapping.devices {
            let machine = format!("{}/{}", device.location, device.name);
            let device_field: Option<String> =
                if device.device_type == DeviceType::Master && !primary_plc_seen {
                    primary_plc_seen = true;
                    None // primary PLC: device=None for backward compat
                } else {
                    // Additional PLC, Slave, and Weigher: device=Some(device_id).
                    // The telemetry loop skips Weigher when building source sessions.
                    Some(device.device_id.clone())
                };
            for (cat, params) in [
                (Category::Monitoring, &device.monitoring),
                (Category::Set, &device.set),
                (Category::Control, &device.control),
            ] {
                for p in params {
                    entries.push(TopicEntry {
                        machine: machine.clone(),
                        device: device_field.clone(),
                        category: cat,
                        key: p.key.clone(),
                        label: p.label.clone(),
                        topic: format!("{}/{}/{}", base_topic, machine, p.topic),
                        data_type: p.data_type,
                        unit: p.unit.clone(),
                        modbus: p.modbus.clone(),
                    });
                }
            }
        }
        let by_topic = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.topic.clone(), i))
            .collect();
        Self { entries, by_topic }
    }

    pub fn lookup(&self, topic: &str) -> Option<&TopicEntry> {
        self.by_topic.get(topic).map(|&i| &self.entries[i])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -------- DataType::parse --------

    #[test]
    fn parse_float_ok() {
        let v = DataType::Float.parse("2.5").unwrap();
        match v {
            TypedValue::Float(f) => assert!((f - 2.5).abs() < 1e-9),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn parse_float_trims_whitespace() {
        let v = DataType::Float.parse("  12.5  \n").unwrap();
        assert!(matches!(v, TypedValue::Float(f) if (f - 12.5).abs() < 1e-9));
    }

    #[test]
    fn parse_float_negative_and_scientific() {
        assert!(matches!(
            DataType::Float.parse("-0.5").unwrap(),
            TypedValue::Float(f) if (f + 0.5).abs() < 1e-9
        ));
        assert!(matches!(
            DataType::Float.parse("1e3").unwrap(),
            TypedValue::Float(f) if (f - 1000.0).abs() < 1e-9
        ));
    }

    #[test]
    fn parse_float_rejects_garbage() {
        assert!(DataType::Float.parse("abc").is_err());
        assert!(DataType::Float.parse("").is_err());
        assert!(DataType::Float.parse("12.5x").is_err());
    }

    #[test]
    fn parse_integer_ok() {
        assert!(matches!(
            DataType::Integer.parse("42").unwrap(),
            TypedValue::Integer(42)
        ));
        assert!(matches!(
            DataType::Integer.parse("-7").unwrap(),
            TypedValue::Integer(-7)
        ));
    }

    #[test]
    fn parse_integer_rejects_float() {
        assert!(DataType::Integer.parse("3.0").is_err());
    }

    #[test]
    fn parse_boolean_truthy() {
        for s in ["true", "TRUE", "True", "1", "on", "ON"] {
            assert!(matches!(
                DataType::Boolean.parse(s).unwrap(),
                TypedValue::Boolean(true)
            ));
        }
    }

    #[test]
    fn parse_boolean_falsy() {
        for s in ["false", "FALSE", "False", "0", "off", "OFF"] {
            assert!(matches!(
                DataType::Boolean.parse(s).unwrap(),
                TypedValue::Boolean(false)
            ));
        }
    }

    #[test]
    fn parse_boolean_rejects_unknown() {
        assert!(DataType::Boolean.parse("yes").is_err());
        assert!(DataType::Boolean.parse("2").is_err());
        assert!(DataType::Boolean.parse("").is_err());
    }

    #[test]
    fn parse_string_passthrough() {
        // DataType::String does not convert — the value is returned as-is.
        let v = DataType::String.parse("kg").unwrap();
        assert!(matches!(v, TypedValue::String(ref s) if s == "kg"));

        let v2 = DataType::String.parse("  lb  ").unwrap();
        assert!(matches!(v2, TypedValue::String(ref s) if s == "lb")); // trim applied

        // String accepts any input, including numbers — not parsed as numeric
        let v3 = DataType::String.parse("123.45").unwrap();
        assert!(matches!(v3, TypedValue::String(ref s) if s == "123.45"));
    }

    #[test]
    fn datatype_string_deserializes_from_json() {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            t: DataType,
        }
        let w: Wrapper = serde_json::from_str(r#"{"t":"string"}"#).unwrap();
        assert_eq!(w.t, DataType::String);
    }

    #[test]
    fn category_as_str_stable() {
        assert_eq!(Category::Monitoring.as_str(), "monitoring");
        assert_eq!(Category::Set.as_str(), "set");
        assert_eq!(Category::Control.as_str(), "control");
    }

    #[test]
    fn param_without_modbus_deserializes_as_none() {
        // A Param without a "modbus" field → modbus = None (most params).
        let json = r#"{
            "key": "actual_pressure",
            "label": "Actual Pressure",
            "topic": "actual/pressure",
            "type": "float",
            "unit": "bar"
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        assert!(p.modbus.is_none());
    }

    #[test]
    fn param_with_read_holding_register_modbus_deserializes() {
        let json = r#"{
            "key": "current_motor",
            "label": "Motor Current",
            "topic": "current/motor",
            "type": "float",
            "unit": "A",
            "modbus": { "kind": "read_holding_registers", "address": 10, "byte_order": "big_big" }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::ReadHoldingRegisters {
                address,
                byte_order,
                ..
            } => {
                assert_eq!(address, 10);
                assert_eq!(byte_order, ByteOrder::BigBig);
            }
            other => panic!("expected ReadHoldingRegisters, got {other:?}"),
        }
    }

    #[test]
    fn param_with_read_coil_modbus_no_byte_order() {
        let json = r#"{
            "key": "status_lamp",
            "label": "Lamp Status",
            "topic": "status/lamp",
            "type": "boolean",
            "modbus": { "kind": "read_coils", "address": 5 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        let binding = p.modbus.expect("modbus present");
        assert!(binding.is_read_coil());
        assert_eq!(binding.address(), 5);
    }

    #[test]
    fn read_holding_register_byte_order_defaults_to_big_big() {
        let json = r#"{
            "key": "x", "label": "X", "topic": "x", "type": "float",
            "modbus": { "kind": "read_holding_registers", "address": 0 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::ReadHoldingRegisters { byte_order, .. } => {
                assert_eq!(byte_order, ByteOrder::BigBig);
            }
            _ => panic!("expected ReadHoldingRegisters"),
        }
    }

    #[test]
    fn modbus_kind_read_input_registers_deserializes() {
        let json = r#"{
            "key": "x", "label": "X", "topic": "x", "type": "float",
            "modbus": { "kind": "read_input_registers", "address": 1000 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        let binding = p.modbus.expect("modbus present");
        assert!(binding.is_read_input_register());
        assert_eq!(binding.address(), 1000);
    }

    #[test]
    fn modbus_kind_write_single_coil_deserializes() {
        let json = r#"{
            "key": "control_lamp", "label": "Lamp On/Off", "topic": "control/lamp",
            "type": "boolean",
            "modbus": { "kind": "write_single_coil", "address": 5 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::WriteSingleCoil { address } => assert_eq!(address, 5),
            other => panic!("expected WriteSingleCoil, got {other:?}"),
        }
    }

    #[test]
    fn modbus_kind_write_single_register_with_on_off_values() {
        let json = r#"{
            "key": "control_motor", "label": "Motor On/Off", "topic": "control/motor",
            "type": "boolean",
            "modbus": { "kind": "write_single_register", "address": 6, "on_value": 1, "off_value": 3 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::WriteSingleRegister {
                address,
                on_value,
                off_value,
            } => {
                assert_eq!(address, 6);
                assert_eq!(on_value, 1);
                assert_eq!(off_value, Some(3));
            }
            other => panic!("expected WriteSingleRegister, got {other:?}"),
        }
    }

    #[test]
    fn write_single_register_off_value_optional() {
        // off_value absent → None (caller defaults to 0).
        let json = r#"{
            "key": "control_valve_up", "label": "Valve Open", "topic": "control/valve/up",
            "type": "boolean",
            "modbus": { "kind": "write_single_register", "address": 4, "on_value": 1 }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::WriteSingleRegister {
                on_value,
                off_value,
                ..
            } => {
                assert_eq!(on_value, 1);
                assert!(off_value.is_none());
            }
            other => panic!("expected WriteSingleRegister, got {other:?}"),
        }
    }

    #[test]
    fn modbus_kind_write_multiple_registers_deserializes() {
        let json = r#"{
            "key": "set_speed", "label": "Speed Setpoint", "topic": "set/speed",
            "type": "float", "unit": "rpm",
            "modbus": { "kind": "write_multiple_registers", "address": 216, "byte_order": "big_little" }
        }"#;
        let p: Param = serde_json::from_str(json).unwrap();
        match p.modbus.expect("modbus present") {
            ModbusBinding::WriteMultipleRegisters {
                address,
                byte_order,
            } => {
                assert_eq!(address, 216);
                assert_eq!(byte_order, ByteOrder::BigLittle);
            }
            other => panic!("expected WriteMultipleRegisters, got {other:?}"),
        }
    }
}

// ===========================================================================
// DeviceMapping (example-mapping.json)
//
// The edge-client mapping format: a `devices` array with a per-device
// connection. `TopicIndex::from_device_mapping` indexes every parameter for
// topic → entry lookup. The primary PLC gets device=None (main bulk read loop),
// additional PLC / slave / weigher get device=Some(device_id).
// ===========================================================================

/// The mapping schema. Detected by the presence of a `"devices"` key in JSON.
/// Weigher scales are expressed as entries in `devices` with
/// `device_type: "weigher"` and `connection.type: "serial_ascii"`.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceMapping {
    pub base_topic: String,
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
    /// All devices: PLC, Modbus slaves, and weigher scales.
    /// Optional — may be empty (e.g. a mapping that only contains weighers).
    #[serde(default)]
    pub devices: Vec<DeviceEntry>,
    #[serde(default)]
    pub bridge: Vec<BridgeRule>,
}

/// One device.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceEntry {
    pub device_type: DeviceType,
    /// Unique device ID, used as a reference in `bridge` rules.
    pub device_id: String,
    pub location: String,
    pub name: String,
    pub connection: DeviceConnection,
    /// Parser config — required for `device_type: "weigher"`, absent for PLC/slave.
    #[serde(default)]
    pub parser: Option<WeigherParser>,
    #[serde(default)]
    pub monitoring: Vec<Param>,
    #[serde(default)]
    pub set: Vec<Param>,
    #[serde(default)]
    pub control: Vec<Param>,
    /// Serial commands triggered from MQTT (only for `device_type: "weigher"`).
    /// The operator publishes to `{base_topic}/{location}/{name}/cmd/{key}` →
    /// edge-client writes `serial_cmd` to the scale's serial port.
    #[serde(default)]
    pub commands: Vec<WeigherCommand>,
}

/// Device kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    /// Master device — determines the `machine_id` and the main Modbus transport.
    Master,
    /// Additional Modbus slave (RS-485, chiller, sensor panel, etc.).
    Slave,
    /// Serial-ASCII weigher scale (GSC, Fujitsu, Mettler, etc.).
    /// Connected via `DeviceConnection::SerialAscii`; does not use Modbus.
    Weigher,
}

fn default_modbus_port() -> u16 {
    502
}
fn default_baud() -> u32 {
    9600
}
fn default_parity() -> String {
    "none".to_string()
}
fn default_stop_bits_u8() -> u8 {
    1
}
fn default_data_bits_u8() -> u8 {
    8
}

/// Parser config for a serial-ASCII weigher scale.
///
/// The regex uses **named capture groups** `(?P<name>...)` — the group name must
/// match a `key` field in `monitoring`. Edge-client iterates over all monitoring
/// entries, looks up the capture group named = key, and publishes its value.
///
/// `byte_map` is applied before ASCII decoding — suited to brands that encode
/// digits as non-standard bytes (e.g. GSC: 0xB0–0xB9 = digits '0'–'9').
/// Leave the array empty if the brand uses standard ASCII.
///
/// GSC example:
/// ```json
/// {
///   "regex": "[A-Za-z-]*(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
///   "byte_map": [
///     {"from": 176, "to": 48}, {"from": 177, "to": 49},
///     {"from": 178, "to": 50}, {"from": 179, "to": 51},
///     {"from": 180, "to": 52}, {"from": 181, "to": 53},
///     {"from": 182, "to": 54}, {"from": 183, "to": 55},
///     {"from": 184, "to": 56}, {"from": 185, "to": 57}
///   ]
/// }
/// ```
///
/// Fujitsu example (standard ASCII, no byte_map needed):
/// ```json
/// {
///   "regex": "WT:\\s*(?P<weight>[0-9.]+)\\s*(?P<unit>\\S+)",
///   "byte_map": []
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct WeigherParser {
    /// Rust-syntax regex with named capture groups. Group name = monitoring key.
    pub regex: String,
    /// Byte substitutions applied before ASCII decode. Empty = no substitution.
    #[serde(default)]
    pub byte_map: Vec<ByteMapEntry>,
    /// Optional: if set, publish all capture groups as JSON to this topic in
    /// addition to the usual per-key publishing.
    /// Full topic: `{base_topic}/{location}/{name}/{raw_topic}`.
    /// Payload: `{"weight": 12.50, "unit": "kg"}` — numeric values are parsed to
    /// numbers, the rest stay strings.
    /// Example: `"raw_topic": "raw"` → publishes to `.../scale_gsc/raw`
    #[serde(default)]
    pub raw_topic: Option<String>,
    /// Optional stability gate: only publish a line when the scale reports a
    /// stable (settled) reading. Requires a named capture group in `regex` for
    /// the status token. Absent = publish every parsed line (default).
    #[serde(default)]
    pub stable: Option<StableRule>,
    /// Optional read-idle watchdog, in milliseconds. If the serial port stays
    /// open but sends nothing for longer than this, the connection is reopened
    /// (guards against a silent-but-open port that would otherwise stall
    /// forever). Leave unset for poll/on-demand scales that are legitimately
    /// idle between reads; set it to a few times the output interval for
    /// continuous-streaming scales.
    #[serde(default)]
    pub read_timeout_ms: Option<u64>,
}

/// Stability gate for a weigher: publish a reading only when the scale reports a
/// stable measurement (not in motion). Many scales emit a status token like
/// `ST` (stable) vs `US`/`MO` (motion) at the start of each line — capture it in
/// `regex` and point this rule at that group.
///
/// Example: `regex` `"(?P<st>ST|US),\\s*(?P<weight>[0-9.]+)"` with
/// `"stable": { "group": "st", "equals": "ST" }` publishes only `ST` lines.
#[derive(Debug, Clone, Deserialize)]
pub struct StableRule {
    /// Name of the regex capture group holding the stability token.
    pub group: String,
    /// The value that means "stable". A line publishes only when the captured
    /// group equals this (after trimming); any other value is skipped.
    pub equals: String,
}

/// One byte-substitution rule: replace a byte valued `from` with `to`.
/// Decimal values (JSON has no hex literal — 0xB0 = 176, 0x30='0' = 48).
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ByteMapEntry {
    pub from: u8,
    pub to: u8,
}

/// A serial command that can be sent to the scale from MQTT.
///
/// The operator publishes to `{base_topic}/{location}/{name}/cmd/{key}` →
/// edge-client writes `serial_cmd` to the scale's serial port.
///
/// `serial_cmd` supports escape sequences: `\r` `\n` `\t` `\\` `\xNN`.
///
/// Example:
/// ```json
/// { "key": "tare",  "serial_cmd": "T\r\n" }
/// { "key": "zero",  "serial_cmd": "Z\r\n" }
/// { "key": "print", "serial_cmd": "P\r\n" }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct WeigherCommand {
    /// Command ID — becomes part of the topic: `cmd/{key}`.
    pub key: String,
    /// Bytes written to the serial port. Escapes: `\r` `\n` `\t` `\\` `\xNN`.
    pub serial_cmd: String,
}

/// Per-device connection. The `"type"` tag matches `connection.type` in JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceConnection {
    /// Standard Modbus TCP. `unit_id` goes into the MBAP header.
    Tcp {
        host: String,
        #[serde(default = "default_modbus_port")]
        port: u16,
        unit_id: u8,
    },
    /// Modbus RTU frames directly to a serial port (USB-RS485, /dev/ttyUSB0, etc.).
    RtuSerial {
        unit_id: u8,
        path: String,
        #[serde(default = "default_baud")]
        baud: u32,
        #[serde(default = "default_parity")]
        parity: String,
        #[serde(default = "default_stop_bits_u8")]
        stop_bits: u8,
        #[serde(default = "default_data_bits_u8")]
        data_bits: u8,
    },
    /// Serial ASCII — for weigher scales. Does not use the Modbus protocol.
    /// Reads text lines, parsed via `WeigherParser.regex`.
    SerialAscii {
        path: String,
        #[serde(default = "default_baud")]
        baud: u32,
        #[serde(default = "default_parity")]
        parity: String,
        #[serde(default = "default_stop_bits_u8")]
        stop_bits: u8,
        #[serde(default = "default_data_bits_u8")]
        data_bits: u8,
    },
}

impl DeviceConnection {
    /// Get the unit_id from a Modbus connection.
    /// Only valid for `Tcp` and `RtuSerial` — must not be called for `SerialAscii`.
    pub fn unit_id(&self) -> u8 {
        match self {
            Self::Tcp { unit_id, .. } | Self::RtuSerial { unit_id, .. } => *unit_id,
            Self::SerialAscii { .. } => {
                panic!("unit_id() called on SerialAscii — a weigher has no Modbus unit_id")
            }
        }
    }
}

impl DeviceMapping {
    /// Detect whether a raw JSON string uses the DeviceMapping format (has a `"devices"` key).
    pub fn is_new_format(raw: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(raw)
            .ok()
            .and_then(|v| v.get("devices").cloned())
            .map(|d| d.is_array())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Tests for DeviceMapping (continued)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod device_mapping_tests {
    use super::*;

    fn example_json() -> &'static str {
        r#"{
            "base_topic": "acme/site",
            "poll_interval_ms": 1000,
            "devices": [
                {
                    "device_type": "master",
                    "device_id": "plc1",
                    "location": "area2",
                    "name": "machine_c",
                    "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                    "monitoring": [
                        { "key": "temp_tank", "label": "Tank Temperature", "topic": "temp/tank",
                          "type": "float", "unit": "C",
                          "modbus": { "kind": "read_holding_registers", "address": 0 } }
                    ],
                    "set": [
                        { "key": "set_temp_tank", "label": "Tank Temp Setpoint", "topic": "set/temp",
                          "type": "float", "unit": "C",
                          "modbus": { "kind": "write_multiple_registers", "address": 10 } }
                    ],
                    "control": []
                },
                {
                    "device_type": "slave",
                    "device_id": "slave1",
                    "location": "area2",
                    "name": "chiller",
                    "connection": {
                        "type": "rtu_serial", "unit_id": 8, "path": "/dev/ttyUSB0",
                        "baud": 19200, "parity": "none", "stop_bits": 1, "data_bits": 8
                    },
                    "monitoring": [
                        { "key": "temp_out_chiller", "label": "Chiller Outlet Temperature",
                          "topic": "temp/out", "type": "float", "unit": "C",
                          "modbus": { "kind": "read_input_registers", "address": 1000, "scale": 0.1 } }
                    ],
                    "set": [],
                    "control": []
                }
            ],
            "bridge": [
                {
                    "read_from": { "device": "slave1", "key": "temp_out_chiller" },
                    "write_to": { "device": "plc1", "key": "set_temp_tank" },
                    "transform": "passthrough"
                }
            ]
        }"#
    }

    #[test]
    fn is_new_format_detects_devices_key() {
        assert!(DeviceMapping::is_new_format(example_json()));
        // A non-mapping object has no "devices"
        let old = r#"{"parameters": {"monitoring": [], "set": [], "control": []}}"#;
        assert!(!DeviceMapping::is_new_format(old));
    }

    #[test]
    fn deserialize_device_mapping() {
        let dm: DeviceMapping = serde_json::from_str(example_json()).unwrap();
        assert_eq!(dm.base_topic, "acme/site");
        assert_eq!(dm.poll_interval_ms, Some(1000));
        assert_eq!(dm.devices.len(), 2);
        assert_eq!(dm.bridge.len(), 1);
    }

    #[test]
    fn device_mapping_plc_has_correct_connection() {
        let dm: DeviceMapping = serde_json::from_str(example_json()).unwrap();
        let plc = dm
            .devices
            .iter()
            .find(|d| d.device_type == DeviceType::Master)
            .unwrap();
        assert_eq!(plc.location, "area2");
        assert_eq!(plc.name, "machine_c");
        assert_eq!(plc.connection.unit_id(), 1);
        assert!(matches!(plc.connection, DeviceConnection::Tcp { .. }));
    }

    #[test]
    fn device_mapping_slave_has_rtu_connection() {
        let dm: DeviceMapping = serde_json::from_str(example_json()).unwrap();
        let slave = dm
            .devices
            .iter()
            .find(|d| d.device_type == DeviceType::Slave)
            .unwrap();
        assert_eq!(slave.location, "area2");
        assert_eq!(slave.name, "chiller");
        assert_eq!(slave.connection.unit_id(), 8);
        assert!(matches!(
            slave.connection,
            DeviceConnection::RtuSerial { .. }
        ));
    }

    #[test]
    fn topic_index_from_device_mapping() {
        let dm: DeviceMapping = serde_json::from_str(example_json()).unwrap();
        let idx = TopicIndex::from_device_mapping("acme/site", &dm);
        // PLC monitoring: {base}/{location}/{name}/{topic} = acme/site/area2/machine_c/temp/tank
        let plc_entry = idx.lookup("acme/site/area2/machine_c/temp/tank");
        assert!(plc_entry.is_some(), "PLC monitoring topic must be in index");
        assert!(
            plc_entry.unwrap().device.is_none(),
            "PLC entries have device=None"
        );
        // Slave monitoring: {base}/{slave_loc}/{slave_name}/{topic} = acme/site/area2/chiller/temp/out
        let slave_entry = idx.lookup("acme/site/area2/chiller/temp/out");
        assert!(
            slave_entry.is_some(),
            "slave monitoring topic must be in index"
        );
        let slave_entry = slave_entry.unwrap();
        assert_eq!(slave_entry.device.as_deref(), Some("slave1"));
        // The Modbus binding must carry through to the TopicEntry (not lost in the index).
        assert_eq!(
            slave_entry
                .modbus
                .as_ref()
                .expect("modbus present")
                .address(),
            1000
        );
    }

    #[test]
    fn device_mapping_bridge_deserializes() {
        let dm: DeviceMapping = serde_json::from_str(example_json()).unwrap();
        assert_eq!(dm.bridge.len(), 1);
        assert_eq!(dm.bridge[0].read_from.device, "slave1");
        assert_eq!(dm.bridge[0].write_to.device, "plc1");
        assert!(matches!(dm.bridge[0].transform, TransformKind::Passthrough));
    }

    #[test]
    fn weigher_device_in_devices_array_deserializes() {
        // A weigher is expressed as an entry in `devices` — not a separate array.
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [
                {
                    "device_type": "weigher",
                    "device_id": "weigher_gsc",
                    "location": "area2",
                    "name": "scale_gsc",
                    "connection": {
                        "type": "serial_ascii",
                        "path": "/dev/ttyUSB1",
                        "baud": 9600,
                        "parity": "none",
                        "stop_bits": 1,
                        "data_bits": 7
                    },
                    "parser": {
                        "regex": "[A-Za-z-]*(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
                        "byte_map": [
                            {"from": 176, "to": 48},
                            {"from": 185, "to": 57}
                        ]
                    },
                    "monitoring": [
                        {"key": "weight", "label": "Weight", "topic": "weight/raw", "type": "float"},
                        {"key": "unit",   "label": "Unit",   "topic": "unit/raw",   "type": "float"}
                    ],
                    "set": [],
                    "control": []
                }
            ]
        }"#;
        let dm: DeviceMapping = serde_json::from_str(json).unwrap();
        assert_eq!(dm.devices.len(), 1);
        let w = &dm.devices[0];
        assert_eq!(w.device_type, DeviceType::Weigher);
        assert_eq!(w.device_id, "weigher_gsc");
        let parser = w.parser.as_ref().expect("parser must exist for a weigher");
        assert_eq!(parser.byte_map.len(), 2);
        assert_eq!(parser.byte_map[0].from, 176);
        assert_eq!(parser.byte_map[0].to, 48);
        assert!(matches!(w.connection, DeviceConnection::SerialAscii { .. }));
        assert_eq!(w.monitoring.len(), 2);
        assert_eq!(w.monitoring[0].key, "weight");
    }

    #[test]
    fn weigher_topic_index_entry_has_device_field() {
        // Weigher entries enter the TopicIndex with device = Some(device_id).
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [{
                "device_type": "weigher",
                "device_id": "weigher_gsc",
                "location": "area2",
                "name": "scale_gsc",
                "connection": {"type": "serial_ascii", "path": "/dev/ttyUSB1"},
                "parser": {"regex": "(?P<weight>[0-9.]+)", "byte_map": []},
                "monitoring": [{"key": "weight", "label": "W", "topic": "weight/raw", "type": "float"}]
            }]
        }"#;
        let dm: DeviceMapping = serde_json::from_str(json).unwrap();
        let idx = TopicIndex::from_device_mapping("acme/site", &dm);
        let e = idx
            .lookup("acme/site/area2/scale_gsc/weight/raw")
            .expect("topic must exist");
        assert_eq!(e.device.as_deref(), Some("weigher_gsc"));
    }

    #[test]
    fn weigher_byte_map_empty_for_ascii_brands() {
        // Standard-ASCII brand (Fujitsu): empty byte_map = no substitution.
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [{
                "device_type": "weigher",
                "device_id": "weigher_fujitsu",
                "location": "area2",
                "name": "scale_fujitsu",
                "connection": {"type": "serial_ascii", "path": "/dev/ttyUSB2"},
                "parser": {
                    "regex": "WT:\\s*(?P<weight>[0-9.]+)\\s*(?P<unit>\\S+)",
                    "byte_map": []
                },
                "monitoring": []
            }]
        }"#;
        let dm: DeviceMapping = serde_json::from_str(json).unwrap();
        let parser = dm.devices[0].parser.as_ref().unwrap();
        assert!(parser.byte_map.is_empty());
    }
}
