//! Weigher scale publisher + command writer.
//!
//! **Read path** (continuous): read ASCII lines from serial, parse via regex,
//! publish each key to MQTT. Optional: combined JSON to `raw_topic`.
//!
//! **Write path** (optional): the operator publishes to
//! `{base_topic}/{location}/{name}/cmd/{key}` → edge-client writes `serial_cmd`
//! to the scale's serial port (tare, zero, print, etc.).
//!
//! Weigher with commands:
//!   1. The port is split into read-half + write-half via `tokio::io::split`.
//!   2. A separate write task receives bytes from `cmd_rx` and writes to serial.
//!   3. `run()` forwards messages from the inbox to the right device via a
//!      per-device channel.
//!
//! A new brand = add an entry in JSON. No binary rebuild needed.

use std::collections::HashMap;
use std::time::Duration;

use crate::shared::{DeviceConnection, DeviceEntry, DeviceType};
use regex::Regex;
use rumqttc::{AsyncClient, QoS};
use serde_json::json;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_serial::SerialPortBuilderExt;
use tracing::{debug, info, warn};

const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Run all weigher loops concurrently.
///
/// `cmd_inbox` — channel from main.rs to receive MQTT command messages
/// (`topic`, `payload`). Only used if there is a weigher with `commands`.
pub async fn run(
    client: AsyncClient,
    base_topic: String,
    devices: Vec<DeviceEntry>,
    qos: QoS,
    mut cmd_inbox: mpsc::Receiver<(String, Vec<u8>)>,
) {
    let weighers: Vec<DeviceEntry> = devices
        .into_iter()
        .filter(|d| d.device_type == DeviceType::Weigher)
        .collect();

    if weighers.is_empty() {
        std::future::pending::<()>().await;
        return;
    }

    // Build: topic_cmd → (device_id, serial_bytes)
    // and: device_id → per-device write channel sender
    let mut topic_to_cmd: HashMap<String, Vec<u8>> = HashMap::new();
    let mut topic_to_sender: HashMap<String, mpsc::Sender<Vec<u8>>> = HashMap::new();

    let mut handles = Vec::with_capacity(weighers.len());

    for device in weighers {
        // Create a per-device channel for write commands to serial
        let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(16);

        // Subscribe + build the topic map for each command entry
        for cmd in &device.commands {
            let topic = format!(
                "{}/{}/{}/cmd/{}",
                base_topic, device.location, device.name, cmd.key
            );
            let bytes = parse_serial_cmd(&cmd.serial_cmd);
            if let Err(e) = client.subscribe(&topic, qos).await {
                warn!(device_id = %device.device_id, cmd = %cmd.key, "subscribe failed: {e:#}");
            } else {
                info!(device_id = %device.device_id, topic = %topic, cmd = %cmd.key, "subscribed cmd topic");
                topic_to_cmd.insert(topic.clone(), bytes);
                topic_to_sender.insert(topic, write_tx.clone());
            }
        }

        let client_c = client.clone();
        let base_c = base_topic.clone();
        handles.push(tokio::spawn(run_one(
            client_c, base_c, device, qos, write_rx,
        )));
    }

    // Demux loop: forward cmd_inbox to the right device
    tokio::spawn(async move {
        while let Some((topic, _payload)) = cmd_inbox.recv().await {
            if let Some((serial_bytes, sender)) =
                topic_to_cmd.get(&topic).zip(topic_to_sender.get(&topic))
                && let Err(e) = sender.send(serial_bytes.clone()).await
            {
                warn!(%topic, "cmd forward failed: {e:#}");
            }
        }
    });

    for h in handles {
        if let Err(e) = h.await {
            warn!("weigher task panicked: {e:?}");
        }
    }
}

/// Read bytes from the reader until `\r`, `\n`, or `\r\n` is found.
///
/// Follows the same logic as the `scale` subcommand:
///   check order `\r\n` → `\n` → `\r` — so `\r\n` is treated as ONE terminator,
///   not two.
///
/// The terminator is **not included** in `buf` — `buf` only holds clean data.
async fn read_line_any(
    reader: &mut (impl AsyncBufRead + Unpin),
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let mut n = 0usize;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(n); // EOF
        }
        let mut consumed = 0;
        for &b in available {
            consumed += 1;
            if b == b'\r' {
                reader.consume(consumed);
                // Peek the next byte — if \n, consume it too (\r\n = one terminator)
                let next = reader.fill_buf().await?;
                if next.first() == Some(&b'\n') {
                    reader.consume(1);
                }
                return Ok(n);
            } else if b == b'\n' {
                reader.consume(consumed);
                return Ok(n);
            } else {
                buf.push(b);
                n += 1;
            }
        }
        reader.consume(consumed);
    }
}

/// One weigher loop: open serial, split read/write, run both paths.
async fn run_one(
    client: AsyncClient,
    base_topic: String,
    device: DeviceEntry,
    qos: QoS,
    mut write_rx: mpsc::Receiver<Vec<u8>>,
) {
    let parser = match &device.parser {
        Some(p) => p.clone(),
        None => {
            warn!(device_id = %device.device_id, "weigher has no parser, task cancelled");
            return;
        }
    };

    let re = match Regex::new(&parser.regex) {
        Ok(r) => r,
        Err(e) => {
            warn!(device_id = %device.device_id, regex = %parser.regex, "invalid regex: {e:#}");
            return;
        }
    };

    let topic_map: HashMap<String, String> = device
        .monitoring
        .iter()
        .map(|p| {
            let t = format!(
                "{}/{}/{}/{}",
                base_topic, device.location, device.name, p.topic
            );
            (p.key.clone(), t)
        })
        .collect();

    let raw_topic: Option<String> = parser
        .raw_topic
        .as_deref()
        .map(|s| format!("{}/{}/{}/{}", base_topic, device.location, device.name, s));

    // Pre-allocate a map for raw_topic publishing — reused per line,
    // clear() is cheaper than Map::new() which allocates a new HashMap.
    let mut raw_map = serde_json::Map::new();

    // Byte lookup table O(1)
    let mut byte_lut = [0u8; 256];
    for i in 0..=255u8 {
        byte_lut[i as usize] = i;
    }
    for rule in &parser.byte_map {
        byte_lut[rule.from as usize] = rule.to;
    }

    let (path, baud, parity_str, stop_bits, data_bits) = match &device.connection {
        DeviceConnection::SerialAscii {
            path,
            baud,
            parity,
            stop_bits,
            data_bits,
        } => (path.clone(), *baud, parity.clone(), *stop_bits, *data_bits),
        _ => {
            warn!(device_id = %device.device_id, "connection is not serial_ascii, task cancelled");
            return;
        }
    };

    loop {
        let port = tokio_serial::new(&path, baud)
            .data_bits(parse_data_bits(data_bits))
            .parity(parse_parity(&parity_str))
            .stop_bits(parse_stop_bits(stop_bits))
            .timeout(Duration::from_millis(500))
            .open_native_async();

        let port = match port {
            Ok(p) => p,
            Err(e) => {
                warn!(device_id = %device.device_id, path = %path, "serial open failed: {e:#}");
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
        };

        info!(device_id = %device.device_id, path = %path, baud, "serial port opened");

        // Split the port into read-half + write-half
        let (read_half, mut write_half) = tokio::io::split(port);
        let mut reader = BufReader::new(read_half);
        let mut buf = Vec::with_capacity(64);

        // ── Read loop + Write task ──────────────────────────────────────────
        loop {
            buf.clear();
            tokio::select! {
                // Read a line from serial — handle \r, \n, or \r\n
                result = read_line_any(&mut reader, &mut buf) => {
                    match result {
                        Ok(0) => { warn!(device_id = %device.device_id, "serial EOF, reopen"); break; }
                        Ok(_) => {}
                        Err(e) => { warn!(device_id = %device.device_id, "serial read error: {e:#}, reopen"); break; }
                    }

                    if buf.is_empty() { continue; }

                    // Byte map + decode ASCII
                    let mapped: Vec<u8> = buf.iter().map(|&b| byte_lut[b as usize]).collect();
                    let s: String = mapped.iter().map(|&b| b as char).collect();
                    let s = s.trim();

                    if s.is_empty() { continue; }

                    // Regex
                    let caps = match re.captures(s) {
                        Some(c) => c,
                        None => {
                            debug!(device_id = %device.device_id, raw_hex = ?buf, line = %s, "no regex match");
                            continue;
                        }
                    };

                    // Publish each key
                    for (key, topic) in &topic_map {
                        if let Some(m) = caps.name(key) {
                            let value = m.as_str().trim();
                            if let Err(e) = client.publish(topic, qos, false, value.as_bytes()).await {
                                warn!(device_id = %device.device_id, topic = %topic, "publish failed: {e:#}");
                            }
                        }
                    }

                    // Publish raw JSON if raw_topic is configured
                    if let Some(ref rt) = raw_topic {
                        raw_map.clear();
                        for name in re.capture_names().flatten() {
                            if let Some(m) = caps.name(name) {
                                let val = m.as_str().trim();
                                let jv = val.parse::<f64>()
                                    .map(|n| json!(n))
                                    .unwrap_or_else(|_| json!(val));
                                raw_map.insert(name.to_string(), jv);
                            }
                        }
                        let payload = serde_json::Value::Object(raw_map.clone()).to_string();
                        if let Err(e) = client.publish(rt, qos, false, payload.into_bytes()).await {
                            warn!(device_id = %device.device_id, "raw publish failed: {e:#}");
                        }
                    }
                }

                // Receive a command from MQTT → write to serial
                Some(cmd_bytes) = write_rx.recv() => {
                    debug!(device_id = %device.device_id, bytes = ?cmd_bytes, "writing cmd to serial");
                    if let Err(e) = write_half.write_all(&cmd_bytes).await {
                        warn!(device_id = %device.device_id, "serial write error: {e:#}");
                        // Don't break — a serial read error is what triggers reopen
                    }
                }
            }
        }

        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

// ---------------------------------------------------------------------------
// Parse escape sequences in serial_cmd: \r \n \t \\ \xNN
// ---------------------------------------------------------------------------

fn parse_serial_cmd(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.extend(c.to_string().as_bytes());
            continue;
        }
        match chars.next() {
            Some('r') => out.push(b'\r'),
            Some('n') => out.push(b'\n'),
            Some('t') => out.push(b'\t'),
            Some('\\') => out.push(b'\\'),
            Some('x') => {
                let h: String = chars.by_ref().take(2).collect();
                if let Ok(b) = u8::from_str_radix(&h, 16) {
                    out.push(b);
                }
            }
            Some(other) => {
                out.push(b'\\');
                out.extend(other.to_string().as_bytes());
            }
            None => out.push(b'\\'),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Serial port helpers
// ---------------------------------------------------------------------------

fn parse_data_bits(bits: u8) -> tokio_serial::DataBits {
    match bits {
        5 => tokio_serial::DataBits::Five,
        6 => tokio_serial::DataBits::Six,
        7 => tokio_serial::DataBits::Seven,
        _ => tokio_serial::DataBits::Eight,
    }
}

fn parse_parity(s: &str) -> tokio_serial::Parity {
    match s.to_ascii_lowercase().trim() {
        "even" | "e" => tokio_serial::Parity::Even,
        "odd" | "o" => tokio_serial::Parity::Odd,
        _ => tokio_serial::Parity::None,
    }
}

fn parse_stop_bits(bits: u8) -> tokio_serial::StopBits {
    match bits {
        2 => tokio_serial::StopBits::Two,
        _ => tokio_serial::StopBits::One,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cmd_crlf() {
        assert_eq!(parse_serial_cmd("T\r\n"), b"T\r\n");
        assert_eq!(parse_serial_cmd("Z\r\n"), b"Z\r\n");
    }

    #[test]
    fn parse_cmd_hex_escape() {
        assert_eq!(parse_serial_cmd("\\x0d\\x0a"), b"\r\n");
    }

    #[test]
    fn parse_cmd_plain() {
        assert_eq!(parse_serial_cmd("TARE"), b"TARE");
    }

    #[test]
    fn parse_cmd_mixed() {
        // "R\r\n" → [b'R', b'\r', b'\n']
        let result = parse_serial_cmd("R\\r\\n");
        assert_eq!(result, b"R\r\n");
    }
}
