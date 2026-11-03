//! Readback state for `set[]` / `control[]`: read the write address back every
//! poll cycle and publish it to [`state_topic`] (`{base}/{machine}/state/{topic}`).
//!
//! A consumer can use these topics as the PLC state, for example to dedup
//! commands. Changes made from an HMI/panel and commands denied by the gate show
//! up there. Only entries with `"readback": true` are read.
//!
//! Runs as its own task, separate from telemetry. A protocol exception (e.g. an
//! address the PLC does not have) makes the actor reconnect and fails the other
//! queued requests, so a device whose readback fails is skipped for
//! [`ERROR_BACKOFF`] instead of reconnecting every second.

use anyhow::{Context as _, Result};
use rumqttc::{AsyncClient, QoS};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::modbus_client::{ActorCache, ReadConn, ReaderFactory};
use crate::settings::Settings;
use crate::shared::{
    ByteOrder, DataType, DeviceEntry, DeviceType, ModbusBinding, Param, state_topic,
};

/// Modbus spec limits per request: FC3 125 registers, FC1 2000 coils.
const MAX_REGISTERS_PER_READ: u16 = 125;
const MAX_COILS_PER_READ: u16 = 2000;

/// Delay before retrying a device whose readback failed.
const ERROR_BACKOFF: Duration = Duration::from_secs(30);

/// Where a readback value comes from, derived from the entry's write binding.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Source {
    /// FC5 coil, read with FC1.
    Coil { address: u16 },
    /// FC6 on/off register, read with FC3. Values other than `on_value` /
    /// `off_value` are not published (state unknown).
    Register {
        address: u16,
        on_value: u16,
        off_value: u16,
    },
    /// FC16 float setpoint (2 registers), read with FC3.
    Float { address: u16, byte_order: ByteOrder },
    /// FC16 integer setpoint (2 registers, i32), read with FC3.
    Integer { address: u16, byte_order: ByteOrder },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadbackItem {
    pub key: String,
    pub state_topic: String,
    pub source: Source,
}

/// One read request: `count` addresses starting at `start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u16,
    pub count: u16,
}

/// Readback for one device: items plus the request plan computed once at startup.
#[derive(Clone)]
pub struct ReadbackDevice {
    pub device_id: String,
    pub reader_factory: ReaderFactory,
    pub items: Vec<ReadbackItem>,
    pub coil_spans: Vec<Span>,
    pub holding_spans: Vec<Span>,
}

/// Build readback for every Modbus device that has `readback: true` entries.
pub async fn build(settings: &Settings, cache: &ActorCache) -> Result<Vec<ReadbackDevice>> {
    let mut devices = Vec::new();
    for device in &settings.mapping.devices {
        if device.device_type == DeviceType::Weigher {
            continue;
        }
        let items = items_for_device(&settings.base_topic, device);
        if items.is_empty() {
            continue;
        }
        let reader_factory = crate::modbus_client::build_source_reader(&device.connection, cache)
            .await
            .with_context(|| format!("build readback reader for '{}'", device.device_id))?;
        let (coil_spans, holding_spans) = plan_spans(&items);
        devices.push(ReadbackDevice {
            device_id: device.device_id.clone(),
            reader_factory,
            items,
            coil_spans,
            holding_spans,
        });
    }
    Ok(devices)
}

/// Readback items from one device's `set[]` + `control[]`. Entries with
/// `readback: true` whose binding cannot be read back are skipped with a warning.
pub fn items_for_device(base_topic: &str, device: &DeviceEntry) -> Vec<ReadbackItem> {
    let machine = format!("{}/{}", device.location, device.name);
    device
        .set
        .iter()
        .chain(&device.control)
        .filter(|p| p.readback)
        .filter_map(|p| {
            let Some(source) = source_for(p) else {
                warn!(
                    device = %device.device_id, key = %p.key,
                    "readback: unsupported binding (needs write_single_coil, \
                     write_single_register, or write_multiple_registers float/integer), skip"
                );
                return None;
            };
            Some(ReadbackItem {
                key: p.key.clone(),
                state_topic: state_topic(base_topic, &machine, &p.topic),
                source,
            })
        })
        .collect()
}

fn source_for(p: &Param) -> Option<Source> {
    match (p.modbus.as_ref()?, p.data_type) {
        (ModbusBinding::WriteSingleCoil { address }, _) => Some(Source::Coil { address: *address }),
        (
            ModbusBinding::WriteSingleRegister {
                address,
                on_value,
                off_value,
            },
            _,
        ) => Some(Source::Register {
            address: *address,
            on_value: *on_value,
            // Same default as the write in control_subscriber.
            off_value: off_value.unwrap_or(0),
        }),
        (
            ModbusBinding::WriteMultipleRegisters {
                address,
                byte_order,
            },
            DataType::Float,
        ) => Some(Source::Float {
            address: *address,
            byte_order: *byte_order,
        }),
        (
            ModbusBinding::WriteMultipleRegisters {
                address,
                byte_order,
            },
            DataType::Integer,
        ) => Some(Source::Integer {
            address: *address,
            byte_order: *byte_order,
        }),
        _ => None,
    }
}

/// Group item addresses into coil and holding-register requests, each under
/// the Modbus per-request limit.
pub fn plan_spans(items: &[ReadbackItem]) -> (Vec<Span>, Vec<Span>) {
    let mut coils = Vec::new();
    let mut holdings = Vec::new();
    for item in items {
        match item.source {
            Source::Coil { address } => coils.push((address, 1)),
            Source::Register { address, .. } => holdings.push((address, 1)),
            Source::Float { address, .. } | Source::Integer { address, .. } => {
                holdings.push((address, 2))
            }
        }
    }
    (
        merge_spans(coils, MAX_COILS_PER_READ),
        merge_spans(holdings, MAX_REGISTERS_PER_READ),
    )
}

/// Greedily merge `(start, len)` ranges while the total span stays <= `max`.
/// Gaps between addresses are read too, like the telemetry bulk read from 0.
fn merge_spans(mut ranges: Vec<(u16, u16)>, max: u16) -> Vec<Span> {
    ranges.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::new();
    for (start, len) in ranges {
        let (s, e) = (start as u32, start as u32 + len as u32);
        match out.last_mut() {
            Some((cs, ce)) if e.max(*ce) - *cs <= max as u32 => *ce = e.max(*ce),
            _ => out.push((s, e)),
        }
    }
    out.into_iter()
        .map(|(s, e)| Span {
            start: s as u16,
            count: (e - s) as u16,
        })
        .collect()
}

/// One cycle's read results, indexed by address.
#[derive(Debug, Default)]
pub struct Snapshot {
    pub coils: HashMap<u16, bool>,
    pub holdings: HashMap<u16, u16>,
}

async fn read_snapshot(
    reader: &mut dyn ReadConn,
    coil_spans: &[Span],
    holding_spans: &[Span],
) -> Result<Snapshot> {
    let mut snap = Snapshot::default();
    for span in coil_spans {
        let bits = reader
            .read_coils_at(span.start, span.count)
            .await
            .with_context(|| format!("read coils {}+{}", span.start, span.count))?;
        for (i, b) in bits.into_iter().enumerate() {
            snap.coils.insert(span.start + i as u16, b);
        }
    }
    for span in holding_spans {
        let regs = reader
            .read_holdings_at(span.start, span.count)
            .await
            .with_context(|| format!("read holdings {}+{}", span.start, span.count))?;
        for (i, r) in regs.into_iter().enumerate() {
            snap.holdings.insert(span.start + i as u16, r);
        }
    }
    Ok(snap)
}

/// State payload for one item, or `None` if the value is not available or does
/// not match `on_value` / `off_value`.
pub fn decode(source: &Source, snap: &Snapshot) -> Option<String> {
    let pair = |a: u16| Some([*snap.holdings.get(&a)?, *snap.holdings.get(&(a + 1))?]);
    match *source {
        Source::Coil { address } => Some(snap.coils.get(&address)?.to_string()),
        Source::Register {
            address,
            on_value,
            off_value,
        } => {
            let v = *snap.holdings.get(&address)?;
            if v == on_value {
                Some("true".into())
            } else if v == off_value {
                Some("false".into())
            } else {
                None
            }
        }
        // `{}` on f32 prints the shortest representation that round-trips, so a
        // written 80.5 reads back as "80.5", not "80.50000000".
        Source::Float {
            address,
            byte_order,
        } => Some(byte_order.decode_f32(pair(address)?).to_string()),
        // Inverse of the control_subscriber encode: i32 bit-cast to u32.
        Source::Integer {
            address,
            byte_order,
        } => Some((byte_order.decode_u32(pair(address)?) as i32).to_string()),
    }
}

/// Run readback forever. Idles if there are no devices.
pub async fn run(client: AsyncClient, devices: Vec<ReadbackDevice>, poll: Duration, qos: QoS) {
    if devices.is_empty() {
        std::future::pending::<()>().await;
        return;
    }
    info!(
        devices = devices.len(),
        items = devices.iter().map(|d| d.items.len()).sum::<usize>(),
        "readback polling starting"
    );

    let mut retry_at: Vec<Option<Instant>> = vec![None; devices.len()];
    let mut ticker = interval(poll);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        for (i, device) in devices.iter().enumerate() {
            if retry_at[i].is_some_and(|t| Instant::now() < t) {
                continue;
            }
            let snap = match device.reader_factory.connect().await {
                Ok(mut reader) => {
                    read_snapshot(reader.as_mut(), &device.coil_spans, &device.holding_spans).await
                }
                Err(e) => Err(e),
            };
            let snap = match snap {
                Ok(s) => {
                    retry_at[i] = None;
                    s
                }
                Err(e) => {
                    warn!(
                        device = %device.device_id,
                        "readback failed: {e:#}, retry in {}s",
                        ERROR_BACKOFF.as_secs()
                    );
                    retry_at[i] = Some(Instant::now() + ERROR_BACKOFF);
                    continue;
                }
            };
            for item in &device.items {
                let Some(payload) = decode(&item.source, &snap) else {
                    debug!(key = %item.key, "readback: unrecognized value, skip");
                    continue;
                };
                if let Err(e) = client
                    .publish(&item.state_topic, qos, false, payload.into_bytes())
                    .await
                {
                    warn!(topic = %item.state_topic, "mqtt publish readback failed: {e:#}");
                }
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::DeviceMapping;
    use async_trait::async_trait;

    fn device_from(json: &str) -> DeviceEntry {
        let m: DeviceMapping = serde_json::from_str(json).unwrap();
        m.devices.into_iter().next().unwrap()
    }

    const MACHINE_A_PLC2: &str = r#"{
        "base_topic": "acme/site",
        "devices": [{
            "device_type": "master", "device_id": "plc2",
            "location": "area1", "name": "machine_a",
            "connection": { "type": "tcp", "host": "192.168.10.10", "port": 503, "unit_id": 1 },
            "set": [
                { "key": "set_speed_mix", "label": "x", "topic": "set/speed/mix", "type": "float",
                  "readback": true,
                  "modbus": { "kind": "write_multiple_registers", "address": 22 } },
                { "key": "set_time_mix", "label": "x", "topic": "set/time/mix", "type": "integer",
                  "readback": true,
                  "modbus": { "kind": "write_multiple_registers", "address": 32 } },
                { "key": "set_no_readback", "label": "x", "topic": "set/other", "type": "float",
                  "modbus": { "kind": "write_multiple_registers", "address": 40 } }
            ],
            "control": [
                { "key": "control_lamp", "label": "x", "topic": "control/lamp", "type": "integer",
                  "readback": true,
                  "modbus": { "kind": "write_single_register", "address": 11, "on_value": 1, "off_value": 3 } },
                { "key": "control_lift_up", "label": "x", "topic": "control/lift/up", "type": "integer",
                  "readback": true,
                  "modbus": { "kind": "write_single_register", "address": 3, "on_value": 4, "off_value": 0 } },
                { "key": "control_lift_down", "label": "x", "topic": "control/lift/down", "type": "integer",
                  "readback": true,
                  "modbus": { "kind": "write_single_register", "address": 3, "on_value": 8, "off_value": 0 } }
            ]
        }]
    }"#;

    #[test]
    fn items_only_for_readback_entries_with_state_topic() {
        let items = items_for_device("acme/site", &device_from(MACHINE_A_PLC2));
        let keys: Vec<&str> = items.iter().map(|i| i.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "set_speed_mix",
                "set_time_mix",
                "control_lamp",
                "control_lift_up",
                "control_lift_down"
            ]
        );
        assert_eq!(
            items[2].state_topic,
            "acme/site/area1/machine_a/state/control/lamp"
        );
        assert_eq!(
            items[2].source,
            Source::Register {
                address: 11,
                on_value: 1,
                off_value: 3
            }
        );
    }

    #[test]
    fn unsupported_binding_is_skipped() {
        // write_multiple_registers with a boolean type has no decode.
        let json = r#"{
            "base_topic": "b",
            "devices": [{
                "device_type": "master", "device_id": "plc1", "location": "l", "name": "n",
                "connection": { "type": "tcp", "host": "h", "port": 502, "unit_id": 1 },
                "control": [
                    { "key": "bad", "label": "x", "topic": "control/bad", "type": "boolean",
                      "readback": true,
                      "modbus": { "kind": "write_multiple_registers", "address": 1 } },
                    { "key": "no_binding", "label": "x", "topic": "control/nb", "type": "boolean",
                      "readback": true }
                ]
            }]
        }"#;
        assert!(items_for_device("b", &device_from(json)).is_empty());
    }

    #[test]
    fn spans_merge_within_limit_and_split_beyond() {
        // Coils 5 and 8212..8228: a single 5..8228 request exceeds 2000.
        let coils = vec![(5, 1), (8212, 1), (8228, 1), (8215, 1)];
        assert_eq!(
            merge_spans(coils, MAX_COILS_PER_READ),
            [
                Span { start: 5, count: 1 },
                Span {
                    start: 8212,
                    count: 17
                }
            ]
        );
        // Setpoints 200..243 (2 registers each) fit in one request.
        let regs = vec![(242, 2), (200, 2), (216, 2)];
        assert_eq!(
            merge_spans(regs, MAX_REGISTERS_PER_READ),
            [Span {
                start: 200,
                count: 44
            }]
        );
        // Exactly at the limit stays one request, one address over becomes two.
        assert_eq!(merge_spans(vec![(0, 1), (124, 1)], 125).len(), 1);
        assert_eq!(merge_spans(vec![(0, 1), (125, 1)], 125).len(), 2);
    }

    #[test]
    fn spans_handle_top_of_address_space() {
        assert_eq!(
            merge_spans(vec![(65534, 2)], MAX_REGISTERS_PER_READ),
            [Span {
                start: 65534,
                count: 2
            }]
        );
    }

    #[test]
    fn plan_spans_shares_register_between_entries() {
        let items = items_for_device("acme/site", &device_from(MACHINE_A_PLC2));
        let (coils, holdings) = plan_spans(&items);
        assert!(coils.is_empty());
        // Registers 3, 11, 22..23 and 32..33 in one request.
        assert_eq!(
            holdings,
            [Span {
                start: 3,
                count: 31
            }]
        );
    }

    fn snap(holdings: &[(u16, u16)], coils: &[(u16, bool)]) -> Snapshot {
        Snapshot {
            holdings: holdings.iter().copied().collect(),
            coils: coils.iter().copied().collect(),
        }
    }

    #[test]
    fn decode_register_on_off_and_unknown() {
        let lamp = Source::Register {
            address: 11,
            on_value: 1,
            off_value: 3,
        };
        assert_eq!(
            decode(&lamp, &snap(&[(11, 1)], &[])).as_deref(),
            Some("true")
        );
        assert_eq!(
            decode(&lamp, &snap(&[(11, 3)], &[])).as_deref(),
            Some("false")
        );
        assert_eq!(decode(&lamp, &snap(&[(11, 0)], &[])), None);
        assert_eq!(decode(&lamp, &snap(&[], &[])), None);
    }

    #[test]
    fn decode_shared_register() {
        let up = Source::Register {
            address: 3,
            on_value: 4,
            off_value: 0,
        };
        let down = Source::Register {
            address: 3,
            on_value: 8,
            off_value: 0,
        };
        let s = snap(&[(3, 4)], &[]);
        assert_eq!(decode(&up, &s).as_deref(), Some("true"));
        // Register 3 = 4 is neither on nor off for `down`, so its state is unknown.
        assert_eq!(decode(&down, &s), None);
        let off = snap(&[(3, 0)], &[]);
        assert_eq!(decode(&up, &off).as_deref(), Some("false"));
        assert_eq!(decode(&down, &off).as_deref(), Some("false"));
    }

    #[test]
    fn decode_coil() {
        let c = Source::Coil { address: 8212 };
        assert_eq!(
            decode(&c, &snap(&[], &[(8212, true)])).as_deref(),
            Some("true")
        );
        assert_eq!(
            decode(&c, &snap(&[], &[(8212, false)])).as_deref(),
            Some("false")
        );
    }

    #[test]
    fn decode_float_and_integer_round_trip_with_write_encoding() {
        let bo = ByteOrder::BigLittle;
        let f = bo.encode_f32(80.5);
        let fs = Source::Float {
            address: 200,
            byte_order: bo,
        };
        assert_eq!(
            decode(&fs, &snap(&[(200, f[0]), (201, f[1])], &[])).as_deref(),
            Some("80.5")
        );

        // Same as the SetInteger encode: i32 bit-cast to u32.
        let i = bo.encode_u32(-5i32 as u32);
        let is = Source::Integer {
            address: 228,
            byte_order: bo,
        };
        assert_eq!(
            decode(&is, &snap(&[(228, i[0]), (229, i[1])], &[])).as_deref(),
            Some("-5")
        );
        // Second register not read: nothing is published.
        assert_eq!(decode(&is, &snap(&[(228, i[0])], &[])), None);
    }

    /// Fake reader: records requests, answers from an address table.
    struct FakeReader {
        holdings: HashMap<u16, u16>,
        coils: HashMap<u16, bool>,
        calls: Vec<String>,
    }

    #[async_trait]
    impl ReadConn for FakeReader {
        async fn read_holdings(&mut self, _count: u16) -> Result<Vec<u16>> {
            unreachable!("readback only uses targeted reads")
        }
        async fn read_coils(&mut self, _count: u16) -> Result<Vec<bool>> {
            unreachable!("readback only uses targeted reads")
        }
        async fn read_input_registers(&mut self, _a: u16, _c: u16) -> Result<Vec<u16>> {
            unreachable!()
        }
        async fn read_discrete_inputs(&mut self, _a: u16, _c: u16) -> Result<Vec<bool>> {
            unreachable!()
        }
        async fn read_holdings_at(&mut self, address: u16, count: u16) -> Result<Vec<u16>> {
            self.calls.push(format!("holdings {address}+{count}"));
            Ok((address..address + count)
                .map(|a| self.holdings.get(&a).copied().unwrap_or(0))
                .collect())
        }
        async fn read_coils_at(&mut self, address: u16, count: u16) -> Result<Vec<bool>> {
            self.calls.push(format!("coils {address}+{count}"));
            Ok((address..address + count)
                .map(|a| self.coils.get(&a).copied().unwrap_or(false))
                .collect())
        }
    }

    #[tokio::test]
    async fn read_snapshot_uses_planned_spans() {
        let items = vec![
            ReadbackItem {
                key: "control_lamp".into(),
                state_topic: "t1".into(),
                source: Source::Coil { address: 5 },
            },
            ReadbackItem {
                key: "control_lift_up".into(),
                state_topic: "t2".into(),
                source: Source::Coil { address: 8212 },
            },
        ];
        let (coil_spans, holding_spans) = plan_spans(&items);
        let mut reader = FakeReader {
            holdings: HashMap::new(),
            coils: [(8212, true)].into_iter().collect(),
            calls: Vec::new(),
        };
        let s = read_snapshot(&mut reader, &coil_spans, &holding_spans)
            .await
            .unwrap();
        assert_eq!(reader.calls, ["coils 5+1", "coils 8212+1"]);
        assert_eq!(decode(&items[0].source, &s).as_deref(), Some("false"));
        assert_eq!(decode(&items[1].source, &s).as_deref(), Some("true"));
    }
}
