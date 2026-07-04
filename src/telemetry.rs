//! Telemetry polling loop: Modbus TCP → decode → MQTT publish per parameter.
//!
//! Strategy:
//! - Build a `TopicIndex` for 1 machine from the JSON mapping, filtering params
//!   that have a `modbus` binding (Some).
//! - Bulk read holding registers `[0, max_holding)` and coils `[0, max_coil)`
//!   per cycle (2 RTT vs N RTT). Decode each param from the buffer.
//! - Publish each param to `{base}/{machine}/{topic}` as a raw scalar string
//!   (one topic per value).
//! - Error handling:
//!   - Connect failed → log + 5s backoff + retry (no crash)
//!   - Read failed mid-loop → break the inner loop, reconnect
//!   - Decode failed per-param → log + skip that param, continue with the others
//!   - Publish failed per-topic → log, continue (the rumqttc queue is handled
//!     internally)

use crate::shared::{Category, DataType, ModbusBinding, TopicEntry, TopicIndex};
use anyhow::{Context as _, Result, anyhow};
use rumqttc::{AsyncClient, QoS};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::modbus_client::{ReadConn, ReaderFactory};
use crate::settings::Settings;

/// Backoff between reconnect attempts — same as `mqtt::POLL_ERROR_BACKOFF`.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Telemetry runtime configuration: derived from `Settings` at startup,
/// immutable for the life of the process. The `entries` field is already
/// filtered to modbus-only.
///
/// The PLC and source devices are polled separately:
/// - PLC: `entries` + `holding_count` + `coil_count`, polled via
///   `reader_factory` (unit_id from the mapping's primary PLC).
/// - Source (chiller, etc.): each entry in `source_sessions`, polled via a
///   `ReaderFactory` already `.with_unit_id(source.unit_id)`. An error on one
///   source does not affect the PLC poll loop or the other sources.
#[derive(Clone)]
pub struct TelemetryConfig {
    pub reader_factory: ReaderFactory,
    pub poll_interval: Duration,
    pub entries: Vec<TopicEntry>,
    /// Number of holding registers to read each cycle.
    /// = max(address)+2 over all HoldingRegisters bindings, or 0 if there are
    /// none.
    pub holding_count: u16,
    /// Number of coils to read each cycle.
    /// = max(address)+1 over all Coils bindings, or 0 if there are none.
    pub coil_count: u16,
    /// One polling session per source device (chiller, etc.). Empty if the
    /// mapping has no `sources`.
    pub source_sessions: Vec<SourcePollSession>,
}

/// Polling state per source device. Independent of the PLC — same bus (TCP) or
/// same actor (RTU) but a different unit_id, with separate error handling.
#[derive(Clone)]
pub struct SourcePollSession {
    /// Device name (the key in `mapping.sources`). Used for logging.
    pub device_name: String,
    /// This device's machine id `{location}/{name}` — used for the per-machine
    /// plc_status topic (`{base}/{machine_id}/plc/status`).
    pub machine_id: String,
    /// ReaderFactory with this source's unit_id (derived via `with_unit_id`).
    pub reader_factory: ReaderFactory,
    /// Holding + coil entries — bulk read from address 0.
    pub entries: Vec<TopicEntry>,
    pub holding_count: u16,
    pub coil_count: u16,
    /// Input register entries (FC4) — targeted read per entry at a specific
    /// address. Not bulk-read from 0 because the address space can be sparse
    /// (e.g. address 1000).
    pub input_entries: Vec<TopicEntry>,
    /// Discrete input entries (FC2) — targeted read per entry, always boolean.
    pub discrete_entries: Vec<TopicEntry>,
}

impl TelemetryConfig {
    pub async fn from_settings(
        settings: &Settings,
        reader_factory: ReaderFactory,
        serial_cache: &crate::modbus_client::SerialActorCache,
    ) -> Result<Self> {
        let modbus = settings
            .modbus
            .as_ref()
            .context("TelemetryConfig::from_settings called without a PLC in the mapping")?;
        // Edge-client = 1 instance per machine, so build the index for 1 machine.
        let idx = TopicIndex::from_device_mapping(&settings.base_topic, &settings.mapping);

        // Filter to params with a modbus binding, monitoring category only.
        // The control category is not published as telemetry readback — the
        // control-state authority is the command history on the consumer side.
        let entries: Vec<TopicEntry> = idx
            .entries
            .iter()
            .filter(|e| {
                e.device.is_none()
                    && e.modbus.is_some()
                    && matches!(e.category, Category::Monitoring)
            })
            .cloned()
            .collect();

        let holding_count = compute_holding_count(&entries);
        let coil_count = compute_coil_count(&entries);

        // Per-source poll session: slave devices + additional PLCs (not the
        // primary PLC). The primary PLC (first in the array) is already handled
        // by the main poll loop above. Additional PLCs (different port, e.g.
        // Siemens multi-port) are polled here as independent source sessions,
        // just like a slave.
        let mut source_sessions: Vec<SourcePollSession> = Vec::new();
        let mut primary_plc_skipped = false;
        for device in &settings.mapping.devices {
            use crate::shared::DeviceType;
            if device.device_type == DeviceType::Weigher {
                continue; // weigher doesn't use Modbus — handled by weigher::run
            }
            if device.device_type == DeviceType::Master && !primary_plc_skipped {
                primary_plc_skipped = true;
                continue; // primary PLC — already handled by the main bulk read loop
            }
            let device_id = &device.device_id;
            let source_entries: Vec<TopicEntry> = idx
                .entries
                .iter()
                .filter(|e| {
                    e.device.as_deref() == Some(device_id.as_str())
                        && e.modbus.is_some()
                        && matches!(e.category, Category::Monitoring)
                })
                .cloned()
                .collect();
            if source_entries.is_empty() {
                continue;
            }
            let source_reader_factory =
                crate::modbus_client::build_source_reader(&device.connection, serial_cache)
                    .await
                    .with_context(|| format!("build source reader for slave '{}'", device_id))?;
            let input_entries: Vec<TopicEntry> = source_entries
                .iter()
                .filter(|e| matches!(e.modbus, Some(ModbusBinding::ReadInputRegisters { .. })))
                .cloned()
                .collect();
            let discrete_entries: Vec<TopicEntry> = source_entries
                .iter()
                .filter(|e| matches!(e.modbus, Some(ModbusBinding::ReadDiscreteInputs { .. })))
                .cloned()
                .collect();
            let holding_coil_entries: Vec<TopicEntry> = source_entries
                .iter()
                .filter(|e| {
                    !matches!(
                        e.modbus,
                        Some(ModbusBinding::ReadInputRegisters { .. })
                            | Some(ModbusBinding::ReadDiscreteInputs { .. })
                    )
                })
                .cloned()
                .collect();
            let s_holding = compute_holding_count(&holding_coil_entries);
            let s_coil = compute_coil_count(&holding_coil_entries);
            source_sessions.push(SourcePollSession {
                device_name: device_id.clone(),
                machine_id: format!("{}/{}", device.location, device.name),
                reader_factory: source_reader_factory,
                entries: holding_coil_entries,
                holding_count: s_holding,
                coil_count: s_coil,
                input_entries,
                discrete_entries,
            });
        }

        Ok(Self {
            reader_factory,
            poll_interval: modbus.poll_interval,
            entries,
            holding_count,
            coil_count,
            source_sessions,
        })
    }
}

/// Compute how many holding registers to read: max address + 2 (because 1 param
/// spans 2 registers), or 0 if there is no holding binding.
fn compute_holding_count(entries: &[TopicEntry]) -> u16 {
    entries
        .iter()
        .filter_map(|e| match e.modbus.as_ref()? {
            ModbusBinding::ReadHoldingRegisters { address, .. } => Some(*address + 2),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// Compute how many coils to read: max address + 1, or 0.
fn compute_coil_count(entries: &[TopicEntry]) -> u16 {
    entries
        .iter()
        .filter_map(|e| match e.modbus.as_ref()? {
            ModbusBinding::ReadCoils { address } => Some(*address + 1),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// Decode 1 param from the bulk-read buffer, then format it as an MQTT string
/// payload. Pure function — no I/O. Independently testable.
///
/// Return value: `Ok(payload)` ready to publish, or `Err` if:
/// - The address is out of the buffer range (the PLC didn't return as many
///   registers as expected)
/// - Mismatch between `data_type` and `modbus.kind` (e.g. coil + integer type)
/// - The param has no `modbus` binding (the caller should filter first)
fn decode_and_format(entry: &TopicEntry, holdings: &[u16], coils: &[bool]) -> Result<String> {
    let binding = entry
        .modbus
        .as_ref()
        .ok_or_else(|| anyhow!("entry without modbus binding (caller should filter)"))?;

    match binding {
        ModbusBinding::ReadHoldingRegisters {
            address,
            byte_order,
            scale,
        } => {
            let a = *address as usize;
            // Spans 2 registers: needs holdings[a] and holdings[a+1].
            if a + 1 >= holdings.len() {
                return Err(anyhow!(
                    "holding address {a} (+1) out of buffer (len={})",
                    holdings.len()
                ));
            }
            let regs = [holdings[a], holdings[a + 1]];
            let raw = byte_order.decode_f32(regs) as f64;
            let scaled = raw * scale.unwrap_or(1.0);
            match entry.data_type {
                DataType::Float => Ok(format!("{scaled:.2}")),
                DataType::Integer => Ok(format!("{}", scaled as i64)),
                DataType::Boolean => Err(anyhow!(
                    "type mismatch: read_holding_registers with a boolean type ({})",
                    entry.key
                )),
                DataType::String => Err(anyhow!(
                    "type mismatch: read_holding_registers with a string type ({})",
                    entry.key
                )),
            }
        }
        ModbusBinding::ReadCoils { address } => {
            let a = *address as usize;
            if a >= coils.len() {
                return Err(anyhow!(
                    "coil address {a} out of buffer (len={})",
                    coils.len()
                ));
            }
            let bit = coils[a];
            match entry.data_type {
                DataType::Boolean => Ok(if bit { "true".into() } else { "false".into() }),
                DataType::Float | DataType::Integer => Err(anyhow!(
                    "type mismatch: read_coils with a non-boolean type ({})",
                    entry.key
                )),
                DataType::String => Err(anyhow!(
                    "type mismatch: read_coils with a string type ({})",
                    entry.key
                )),
            }
        }
        ModbusBinding::ReadInputRegisters { .. } => Err(anyhow!(
            "read_input_registers entries are decoded inline, not via decode_and_format ({})",
            entry.key
        )),
        ModbusBinding::ReadDiscreteInputs { .. } => Err(anyhow!(
            "read_discrete_inputs entries are decoded inline, not via decode_and_format ({})",
            entry.key
        )),
        ModbusBinding::WriteSingleCoil { .. }
        | ModbusBinding::WriteSingleRegister { .. }
        | ModbusBinding::WriteMultipleRegisters { .. } => Err(anyhow!(
            "a write binding in a monitoring entry is not valid for decode ({})",
            entry.key
        )),
    }
}

/// Publish 1 batch of entries (PLC or 1 source device) to MQTT. Pure publish
/// logic — the caller provides the holdings/coils buffer from the bulk read.
/// Does not break the loop when a per-entry decode/publish fails; it accumulates
/// an error count and continues.
async fn publish_entries(
    client: &AsyncClient,
    entries: &[TopicEntry],
    holdings: &[u16],
    coils: &[bool],
    qos: QoS,
) {
    let mut ok = 0_usize;
    let mut errs = 0_usize;
    for entry in entries {
        match decode_and_format(entry, holdings, coils) {
            Ok(payload) => {
                match client
                    .publish(&entry.topic, qos, false, payload.into_bytes())
                    .await
                {
                    Ok(()) => ok += 1,
                    Err(e) => {
                        warn!(topic = %entry.topic, "mqtt publish failed: {e:#}");
                        errs += 1;
                    }
                }
            }
            Err(e) => {
                debug!(key = %entry.key, "decode skipped: {e:#}");
                errs += 1;
            }
        }
    }
    debug!(published = ok, errors = errs, "telemetry batch done");
}

/// Poll one source-device tick: ensure the reader is connected (lazy), bulk
/// read, publish. Any error → drop the reader (next tick reconnects). Does not
/// return an error to the caller — a source failure must not trigger a PLC
/// reconnect.
async fn poll_and_publish_source(
    session: &SourcePollSession,
    reader_slot: &mut Option<Box<dyn ReadConn>>,
    status_tx: &watch::Sender<bool>,
    client: &AsyncClient,
    qos: QoS,
) {
    if reader_slot.is_none() {
        match session.reader_factory.connect().await {
            Ok(r) => {
                info!(device = %session.device_name, "source reader connected");
                *reader_slot = Some(r);
                let _ = status_tx.send(true);
            }
            Err(e) => {
                warn!(
                    device = %session.device_name,
                    "source connect failed: {e:#}, skip cycle"
                );
                let _ = status_tx.send(false);
                return;
            }
        }
    }
    // Safe unwrap: just inserted / already Some.
    let reader = reader_slot.as_mut().unwrap();
    let holdings = match reader.read_holdings(session.holding_count).await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                device = %session.device_name,
                "source read holdings failed: {e:#}, drop reader"
            );
            *reader_slot = None;
            let _ = status_tx.send(false);
            return;
        }
    };
    let coils = match reader.read_coils(session.coil_count).await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                device = %session.device_name,
                "source read coils failed: {e:#}, drop reader"
            );
            *reader_slot = None;
            let _ = status_tx.send(false);
            return;
        }
    };
    publish_entries(client, &session.entries, &holdings, &coils, qos).await;

    // FC4 input registers — targeted read per entry (address can be sparse).
    //
    // Two decode modes based on the presence of `scale`:
    // - `scale: Some(x)` → read 1 register, result is `raw as f64 * x`. Used for
    //   PLCs that store the value as a scaled integer (e.g. chiller: 250 → 25.0°C).
    // - `scale: None` → read 2 registers, decode as IEEE 754 float32 via
    //   `byte_order.decode_f32`. For PLCs that store a float in the input
    //   register space. Integer without scale: decode as u32 via
    //   `byte_order.decode_u32`.
    // Boolean is always 1 register (scale and byte_order are ignored).
    for entry in &session.input_entries {
        let Some(ModbusBinding::ReadInputRegisters {
            address,
            scale,
            byte_order,
        }) = &entry.modbus
        else {
            continue;
        };
        let count: u16 = match entry.data_type {
            DataType::Float | DataType::Integer if scale.is_none() => 2,
            _ => 1,
        };
        match reader.read_input_registers(*address, count).await {
            Ok(regs) if regs.len() >= count as usize => {
                let payload = match entry.data_type {
                    DataType::Float if scale.is_none() => {
                        // 2-register IEEE 754 decode (like HoldingRegisters).
                        let v = byte_order.decode_f32([regs[0], regs[1]]);
                        format!("{v:.2}")
                    }
                    DataType::Integer if scale.is_none() => {
                        // 2-register unsigned integer decode.
                        let v = byte_order.decode_u32([regs[0], regs[1]]) as i64;
                        format!("{v}")
                    }
                    DataType::Float => {
                        // 1-register scaled integer (e.g. chiller: raw=250, scale=0.1 → 25.0).
                        let v = regs[0] as f64 * scale.unwrap_or(1.0);
                        format!("{v:.2}")
                    }
                    DataType::Integer => {
                        let v = (regs[0] as f64 * scale.unwrap_or(1.0)) as i64;
                        format!("{v}")
                    }
                    DataType::Boolean => {
                        if regs[0] != 0 {
                            "true".into()
                        } else {
                            "false".into()
                        }
                    }
                    DataType::String => {
                        // A string type has no Modbus binding — it never reaches
                        // here because the from_settings filter excludes entries
                        // without a modbus binding before this loop.
                        warn!(key = %entry.key, "string type in input_registers, skip");
                        continue;
                    }
                };
                if let Err(e) = client
                    .publish(&entry.topic, qos, false, payload.into_bytes())
                    .await
                {
                    warn!(topic = %entry.topic, "mqtt publish input_register failed: {e:#}");
                }
            }
            Ok(_) => {
                warn!(device = %session.device_name, key = %entry.key, "input register read returned empty");
            }
            Err(e) => {
                warn!(
                    device = %session.device_name,
                    key = %entry.key,
                    "input register read failed: {e:#}, drop reader"
                );
                *reader_slot = None;
                let _ = status_tx.send(false);
                return;
            }
        }
    }

    // FC2 discrete inputs — targeted read per entry, always boolean.
    for entry in &session.discrete_entries {
        let Some(ModbusBinding::ReadDiscreteInputs { address }) = &entry.modbus else {
            continue;
        };
        match reader.read_discrete_inputs(*address, 1).await {
            Ok(bits) if !bits.is_empty() => {
                let payload = if bits[0] { "true" } else { "false" };
                if let Err(e) = client
                    .publish(&entry.topic, qos, false, payload.as_bytes())
                    .await
                {
                    warn!(topic = %entry.topic, "mqtt publish discrete_input failed: {e:#}");
                }
            }
            Ok(_) => {
                warn!(device = %session.device_name, key = %entry.key, "discrete input read returned empty");
            }
            Err(e) => {
                warn!(
                    device = %session.device_name,
                    key = %entry.key,
                    "discrete input read failed: {e:#}, drop reader"
                );
                *reader_slot = None;
                let _ = status_tx.send(false);
                return;
            }
        }
    }
}

/// Run the polling loop forever. The caller embeds it in a `tokio::select!`.
///
/// Lifecycle per "session":
/// 1. Connect Modbus TCP (PLC reader)
/// 2. Loop: tick interval → bulk read PLC → decode → publish, then poll each
///    source session (independent — an error on one source doesn't trigger a PLC
///    reconnect)
/// 3. On a PLC read error, drop the reader (auto-disconnect via Drop) + backoff +
///    return to step 1. Source readers are cached outside the outer loop so
///    per-source state survives a PLC reconnect.
///
/// `status_tx`: primary PLC connection status (`true`=connected).
/// `source_status_tx`: one sender per source session (order =
/// `config.source_sessions`), for each source device's connection status.
/// `plc_status::run` (one per machine) listens on these channels and publishes
/// to MQTT every second.
pub async fn run(
    client: AsyncClient,
    config: TelemetryConfig,
    qos: QoS,
    status_tx: watch::Sender<bool>,
    source_status_tx: Vec<watch::Sender<bool>>,
) {
    if config.entries.is_empty() && config.source_sessions.is_empty() {
        warn!(
            "no parameter has a modbus binding (PLC or source) — telemetry loop idle. \
             Check mapping.json if this is unexpected."
        );
        // Idle forever — nothing to read. But don't exit, so the
        // tokio::select! in main doesn't panic if telemetry "finishes".
        std::future::pending::<()>().await;
        return;
    }

    info!(
        active_params = config.entries.len(),
        holding_count = config.holding_count,
        coil_count = config.coil_count,
        source_sessions = config.source_sessions.len(),
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "telemetry polling starting"
    );

    // Cache a reader per source. Lazy-connect on first poll, drop on error →
    // reconnect next tick. Not using the `vec![None; N]` macro because
    // `Box<dyn ReadConn>` isn't Clone (the vec! macro needs Clone via
    // expansion). (0..N).map(|_| None).collect() bypasses the Clone requirement.
    let mut source_readers: Vec<Option<Box<dyn ReadConn>>> =
        (0..config.source_sessions.len()).map(|_| None).collect();

    // Send "disconnected" once at the start — before the first connect.
    // plc_status::run publishes to MQTT as soon as it receives this signal.
    let _ = status_tx.send(false);

    loop {
        let mut reader = match config.reader_factory.connect().await {
            Ok(r) => {
                info!("modbus connected (PLC reader), starting poll cycle");
                let _ = status_tx.send(true);
                r
            }
            Err(e) => {
                warn!(
                    "modbus connect failed (PLC): {e:#}, retry in {}s",
                    RECONNECT_BACKOFF.as_secs()
                );
                let _ = status_tx.send(false);
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
        };

        let mut ticker = interval(config.poll_interval);
        // Delay missed ticks so a slow poll doesn't cause a tick burst catch-up.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Inner loop = 1 PLC connection lifecycle. Break to the outer loop if the
        // PLC read fails.
        loop {
            ticker.tick().await;

            // --- PLC poll ---
            let holdings = match reader.read_holdings(config.holding_count).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("modbus PLC read holdings failed: {e:#}, reconnect");
                    break;
                }
            };
            let coils = match reader.read_coils(config.coil_count).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("modbus PLC read coils failed: {e:#}, reconnect");
                    break;
                }
            };
            publish_entries(&client, &config.entries, &holdings, &coils, qos).await;

            // --- Source polls (independent) ---
            for (i, session) in config.source_sessions.iter().enumerate() {
                poll_and_publish_source(
                    session,
                    &mut source_readers[i],
                    &source_status_tx[i],
                    &client,
                    qos,
                )
                .await;
            }
        }

        // Inner loop exited via break = PLC reconnect after backoff.
        // Source readers stay cached; they reconnect lazily on the next tick
        // after the PLC is up.
        let _ = status_tx.send(false);
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

// ===========================================================================
// Tests — focused on pure decode/format. The polling loop is manually smoke-tested.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{ByteOrder, Category};

    fn holding_entry(key: &str, data_type: DataType, address: u16) -> TopicEntry {
        TopicEntry {
            machine: "area1/machine_a".into(),
            device: None,
            category: Category::Monitoring,
            key: key.into(),
            label: key.into(),
            topic: format!("acme/site/area1/machine_a/{key}"),
            data_type,
            unit: None,
            modbus: Some(ModbusBinding::ReadHoldingRegisters {
                address,
                byte_order: ByteOrder::BigBig,
                scale: None,
            }),
        }
    }

    fn coil_entry(key: &str, address: u16) -> TopicEntry {
        TopicEntry {
            machine: "area1/machine_a".into(),
            device: None,
            category: Category::Monitoring,
            key: key.into(),
            label: key.into(),
            topic: format!("acme/site/area1/machine_a/{key}"),
            data_type: DataType::Boolean,
            unit: None,
            modbus: Some(ModbusBinding::ReadCoils { address }),
        }
    }

    #[test]
    fn decode_holding_float_formats_two_decimals() {
        // 75.5f32 = 0x42970000 in big_big = regs [0x4297, 0x0000]
        let entry = holding_entry("temp_tank", DataType::Float, 0);
        let holdings = vec![0x4297, 0x0000];
        let coils = vec![];
        let out = decode_and_format(&entry, &holdings, &coils).unwrap();
        // f32 75.5 with {:.2} → "75.50"
        assert_eq!(out, "75.50");
    }

    #[test]
    fn decode_holding_integer_casts_from_float() {
        // run_time: previously decoded as float, current type=integer → cast as i64
        // 12.5f32 = 0x41480000 in big_big = regs [0x4148, 0x0000]
        let entry = holding_entry("run_time", DataType::Integer, 6);
        let mut holdings = vec![0u16; 8];
        holdings[6] = 0x4148;
        holdings[7] = 0x0000;
        let coils = vec![];
        let out = decode_and_format(&entry, &holdings, &coils).unwrap();
        // 12.5 as i64 = 12 (truncate)
        assert_eq!(out, "12");
    }

    #[test]
    fn decode_holding_float_with_scale_applied() {
        // Param with scale: 0.1 — raw 750.0 → publish "75.00"
        // 750.0f32 = 0x443B8000 → big_big regs [0x443B, 0x8000]
        let entry = TopicEntry {
            machine: "area3/machine_c".into(),
            device: None,
            category: Category::Monitoring,
            key: "example_scaled".into(),
            label: "Example Scaled".into(),
            topic: "acme/site/area3/machine_c/example".into(),
            data_type: DataType::Float,
            unit: None,
            modbus: Some(ModbusBinding::ReadHoldingRegisters {
                address: 0,
                byte_order: ByteOrder::BigBig,
                scale: Some(0.1),
            }),
        };
        let holdings = vec![0x443Bu16, 0x8000];
        let out = decode_and_format(&entry, &holdings, &[]).unwrap();
        // 750.0f32 * 0.1 = 75.0 → "75.00"
        assert_eq!(out, "75.00");
    }

    #[test]
    fn decode_holding_scale_none_is_noop() {
        // scale: None must be identical to scale: Some(1.0) — backward-compat
        // 75.5f32 = 0x42970000 → big_big regs [0x4297, 0x0000]
        let entry_no_scale = holding_entry("temp_tank", DataType::Float, 0);
        let entry_scale_one = TopicEntry {
            modbus: Some(ModbusBinding::ReadHoldingRegisters {
                address: 0,
                byte_order: ByteOrder::BigBig,
                scale: Some(1.0),
            }),
            ..holding_entry("temp_tank", DataType::Float, 0)
        };
        let holdings = vec![0x4297u16, 0x0000];
        let out_none = decode_and_format(&entry_no_scale, &holdings, &[]).unwrap();
        let out_one = decode_and_format(&entry_scale_one, &holdings, &[]).unwrap();
        assert_eq!(out_none, "75.50");
        assert_eq!(
            out_none, out_one,
            "scale:None must be identical to scale:Some(1.0)"
        );
    }

    #[test]
    fn decode_coil_boolean_true_and_false() {
        let entry_on = coil_entry("control_lamp", 5);
        let entry_off = coil_entry("control_heater", 0);
        let coils = vec![false, false, false, false, false, true, false];
        assert_eq!(decode_and_format(&entry_on, &[], &coils).unwrap(), "true");
        assert_eq!(decode_and_format(&entry_off, &[], &coils).unwrap(), "false");
    }

    #[test]
    fn decode_holding_address_out_of_bounds_errors() {
        // Param at addr 10 but the buffer only has 4 registers → addr 10+1=11 out
        let entry = holding_entry("current_motor", DataType::Float, 10);
        let holdings = vec![0u16; 4];
        let err = decode_and_format(&entry, &holdings, &[])
            .expect_err("must error on out-of-bounds address");
        assert!(
            err.to_string().contains("out of buffer"),
            "expected 'out of buffer' in error, got: {err}"
        );
    }

    #[test]
    fn decode_coil_address_out_of_bounds_errors() {
        let entry = coil_entry("control_heater", 68);
        let coils = vec![false; 10];
        let err =
            decode_and_format(&entry, &[], &coils).expect_err("must error on out-of-bounds coil");
        assert!(err.to_string().contains("out of buffer"));
    }

    #[test]
    fn decode_type_mismatch_coil_with_float_errors() {
        // Defense: catch misconfiguration in the mapping JSON
        let mut entry = coil_entry("bad_param", 0);
        entry.data_type = DataType::Float;
        let coils = vec![true];
        let err = decode_and_format(&entry, &[], &coils)
            .expect_err("must reject coil binding with non-boolean type");
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn compute_counts_max_address_plus_size() {
        let entries = vec![
            holding_entry("a", DataType::Float, 0),  // span 0-1
            holding_entry("b", DataType::Float, 12), // span 12-13
            coil_entry("c", 0),
            coil_entry("d", 68),
        ];
        // Max holding addr = 12, span 2 → count = 14
        assert_eq!(compute_holding_count(&entries), 14);
        // Max coil addr = 68 → count = 69
        assert_eq!(compute_coil_count(&entries), 69);
    }

    #[test]
    fn compute_counts_zero_when_no_bindings() {
        let entries: Vec<TopicEntry> = vec![];
        assert_eq!(compute_holding_count(&entries), 0);
        assert_eq!(compute_coil_count(&entries), 0);
    }

    /// The telemetry polling loop publishes only Category::Monitoring, not
    /// Category::Control — control readback is owned by control_subscriber.
    /// The filter predicate is the same one used in
    /// `TelemetryConfig::from_settings`.
    #[test]
    fn from_settings_filter_excludes_control_category() {
        let write_coil_entry = |key: &str, address: u16| TopicEntry {
            machine: "area1/machine_a".into(),
            device: None,
            category: Category::Control,
            key: key.into(),
            label: key.into(),
            topic: format!("acme/site/area1/machine_a/{key}"),
            data_type: DataType::Boolean,
            unit: None,
            modbus: Some(ModbusBinding::WriteSingleCoil { address }),
        };
        let entries = [
            holding_entry("current_motor", DataType::Float, 10), // Monitoring + read binding → include
            write_coil_entry("control_lamp", 5),                 // Control + write binding → skip
            write_coil_entry("control_motor", 6),                // Control + write binding → skip
        ];
        let filtered: Vec<&TopicEntry> = entries
            .iter()
            .filter(|e| {
                e.device.is_none()
                    && e.modbus.is_some()
                    && matches!(e.category, Category::Monitoring)
            })
            .collect();
        assert_eq!(filtered.len(), 1, "only monitoring entry should survive");
        assert_eq!(filtered[0].key, "current_motor");
    }

    /// Source-device monitoring entries are separated from the PLC via the
    /// `device` field. `device.is_none()` = PLC, `device == Some(name)` = source.
    /// The `from_settings::source_sessions` build uses the second predicate.
    #[test]
    fn source_monitoring_entries_partition_by_device_field() {
        use crate::shared::DeviceMapping;
        let json = r#"{
            "base_topic": "acme/site",
            "devices": [
                {
                    "device_type": "master", "device_id": "plc1",
                    "location": "area2", "name": "machine_c",
                    "connection": { "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 },
                    "monitoring": [
                        { "key": "current_motor", "label": "Motor Current", "topic": "current/motor",
                          "type": "float", "unit": "A",
                          "modbus": { "kind": "read_holding_registers", "address": 10 } }
                    ],
                    "set": [], "control": []
                },
                {
                    "device_type": "slave", "device_id": "slave1",
                    "location": "area2", "name": "chiller",
                    "connection": { "type": "rtu_serial", "unit_id": 2, "path": "/dev/ttyUSB0",
                                    "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
                    "monitoring": [{
                        "key": "temp_out_chiller", "label": "Chiller Outlet Temperature",
                        "topic": "temp/out", "type": "float", "unit": "C",
                        "modbus": { "kind": "read_holding_registers", "address": 0 }
                    }],
                    "set": [], "control": []
                }
            ]
        }"#;
        let m: DeviceMapping = serde_json::from_str(json).unwrap();
        let idx = crate::shared::TopicIndex::from_device_mapping("acme/site", &m);

        // PLC partition
        let plc: Vec<&TopicEntry> = idx
            .entries
            .iter()
            .filter(|e| {
                e.device.is_none()
                    && e.modbus.is_some()
                    && matches!(e.category, Category::Monitoring)
            })
            .collect();
        assert_eq!(plc.len(), 1, "1 PLC monitoring entry");
        assert_eq!(plc[0].key, "current_motor");

        // Chiller partition — device field = device_id ("slave1"), not name ("chiller").
        // The topic still uses name: acme/site/area2/chiller/temp/out.
        let chiller: Vec<&TopicEntry> = idx
            .entries
            .iter()
            .filter(|e| {
                e.device.as_deref() == Some("slave1")
                    && e.modbus.is_some()
                    && matches!(e.category, Category::Monitoring)
            })
            .collect();
        assert_eq!(chiller.len(), 1, "1 chiller monitoring entry");
        assert_eq!(chiller[0].key, "temp_out_chiller");
        assert_eq!(chiller[0].topic, "acme/site/area2/chiller/temp/out");
    }
}
