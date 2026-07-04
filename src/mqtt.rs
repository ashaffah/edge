//! Thin wrapper over `rumqttc` for edge-client.
//!
//! Goals:
//! 1. Build an `AsyncClient` with an LWT (last-will-testament) that publishes
//!    `{"status":"disconnected"}` automatically if the edge crashes or the
//!    network drops.
//! 2. Drive the event loop with error tolerance — rumqttc handles
//!    auto-reconnect, we just log and back off so we don't busy-loop while the
//!    broker is down.
//! 3. Expose topic helpers so other features (heartbeat, resource, telemetry
//!    publishers) don't duplicate format strings.

use anyhow::Result;
use rumqttc::{
    AsyncClient, Event, EventLoop, LastWill, MqttOptions, Packet, Publish, QoS, Transport,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::settings::Settings;

/// rumqttc queue capacity for publishes while the broker is disconnected.
/// 100 = ~100 seconds of heartbeats (1/s) buffered before the oldest is dropped.
const CLIENT_QUEUE_CAP: usize = 100;

/// Backoff on eventloop error so we don't busy-loop with log spam.
const POLL_ERROR_BACKOFF: Duration = Duration::from_secs(5);

/// LWT payload. Static — the broker publishes this if the client disconnects
/// abnormally (network drop, process crash). Same content as the heartbeat
/// payload (`{ status }`).
const LWT_PAYLOAD: &[u8] = br#"{"status":"disconnected"}"#;

/// Heartbeat topic for the primary machine_id. Used by the LWT (set once at
/// connect).
pub fn heartbeat_topic(settings: &Settings) -> String {
    format!("{}/{}/heartbeat", settings.base_topic, settings.machine_id)
}

/// Heartbeat topics for ALL unique machines (location/name) in the mapping. One
/// instance can serve several machines (multi-PLC/slave/weigher) — each machine
/// gets its own heartbeat so the dashboard knows the status per machine. Device
/// order is preserved; duplicate location/name pairs are skipped. The LWT still
/// only covers the primary machine_id (`heartbeat_topic`).
pub fn heartbeat_topics(settings: &Settings) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut topics = Vec::new();
    for d in &settings.mapping.devices {
        let machine = format!("{}/{}", d.location, d.name);
        if seen.insert(machine.clone()) {
            topics.push(format!("{}/{}/heartbeat", settings.base_topic, machine));
        }
    }
    // Defensive fallback: deriving machine_id requires at least 1 device, so
    // topics should never be empty — but just in case.
    if topics.is_empty() {
        topics.push(heartbeat_topic(settings));
    }
    topics
}

/// Resource topics (CPU/memory/IP snapshot) for ALL unique machines
/// (location/name) in the mapping — same as [`heartbeat_topics`].
///
/// Uses the full `machine_id` (`location/name`) so each machine has its own
/// unique resources topic. Truncating to just the location would collide if one
/// location has >1 edge-client (e.g. area1: 2 boards → machine_a & machine_b).
///
/// If one edge-client serves >1 PLC (e.g. line1: area2 + area3), each machine
/// gets its own resources. The CPU/mem/IP data is the same (one board), so it's
/// redundant — accepted so the dashboard has a resource entry per machine.
pub fn resource_topics(settings: &Settings) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut topics = Vec::new();
    for d in &settings.mapping.devices {
        let machine = format!("{}/{}", d.location, d.name);
        if seen.insert(machine.clone()) {
            topics.push(format!("{}/{}/resources", settings.base_topic, machine));
        }
    }
    // Defensive fallback: the mapping has at least 1 device, but just in case.
    if topics.is_empty() {
        topics.push(format!(
            "{}/{}/resources",
            settings.base_topic, settings.machine_id
        ));
    }
    topics
}

/// PLC connection status topic for this machine.
pub fn plc_status_topic(settings: &Settings) -> String {
    format!("{}/{}/plc/status", settings.base_topic, settings.machine_id)
}

/// Convert `MQTT_PUBLISH_QOS` (u8) to `rumqttc::QoS`. Defaults to at-least-once
/// for invalid values (rather than panicking).
pub fn publish_qos(settings: &Settings) -> QoS {
    match settings.mqtt.publish_qos {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

/// Build the MQTT client + event loop. Not yet connected — connection happens
/// implicitly once `drive_eventloop` starts polling.
///
/// Uses `MqttOptions::new` + manual `set_transport` (not `parse_url`) because in
/// rumqttc 0.25 `parse_url` for ws/wss only keeps `host_str()` without the
/// scheme, so the eventloop fails to parse the URL during the WS handshake
/// (rumqttc issue #808). The manual switch avoids this bug entirely.
///
/// Transport per protocol:
/// - `mqtt`  → TCP (default, no need to set_transport)
/// - `mqtts` → TLS via rustls with webpki-roots
/// - `ws`    → plain WebSocket, the host arg = full URL `ws://host:port/path`
/// - `wss`   → WebSocket Secure, the host arg = full URL `wss://host:port/path`
pub fn build_client(settings: &Settings) -> Result<(AsyncClient, EventLoop)> {
    let topic = heartbeat_topic(settings);
    let m = &settings.mqtt;

    let ws_path = m.websocket_path.as_deref().unwrap_or("/mqtt");

    // For ws/wss, rumqttc needs the full URL as the host argument so its
    // internal WS handshake can parse the scheme + path correctly.
    let (host_arg, transport) = match m.protocol.as_str() {
        "mqtt" => (m.host.clone(), None),
        "mqtts" => (m.host.clone(), Some(Transport::tls_with_default_config())),
        "ws" => (
            format!("ws://{}:{}{}", m.host, m.port, ws_path),
            Some(Transport::Ws),
        ),
        "wss" => (
            format!("wss://{}:{}{}", m.host, m.port, ws_path),
            Some(Transport::wss_with_default_config()),
        ),
        // settings.rs already validates this enum — this branch is unreachable.
        other => anyhow::bail!("MQTT_PROTOCOL '{other}' is not recognized"),
    };

    let mut opts = MqttOptions::new(&m.client_id, &host_arg, m.port);
    opts.set_keep_alive(std::time::Duration::from_secs(m.keepalive_secs));
    if let Some(t) = transport {
        opts.set_transport(t);
    }
    opts.set_credentials(&m.username, &m.password);

    // LWT retained=true so a new subscriber (a dashboard/consumer that comes up
    // after a restart) immediately sees the last status without waiting for the
    // next heartbeat.
    opts.set_last_will(LastWill::new(
        &topic,
        LWT_PAYLOAD,
        publish_qos(settings),
        true,
    ));

    let (client, eventloop) = AsyncClient::new(opts, CLIENT_QUEUE_CAP);
    let broker_display = match m.protocol.as_str() {
        p if p.starts_with("ws") => format!("{}://{}:{}{}", m.protocol, m.host, m.port, ws_path),
        _ => format!("{}://{}:{}", m.protocol, m.host, m.port),
    };
    info!(
        broker = %broker_display,
        protocol = %m.protocol,
        client_id = %m.client_id,
        lwt_topic = %topic,
        "MQTT client built (event loop must be driven to connect)"
    );
    Ok((client, eventloop))
}

/// Drive the event loop forever. Does not return normally — embed it in a
/// `tokio::select!` with a shutdown signal.
///
/// rumqttc handles reconnect automatically with internal exponential backoff.
/// We add extra backoff here only to avoid log spam while the broker is truly
/// offline for a long time.
///
/// `inbox`: if `Some`, every `Event::Incoming(Publish)` is forwarded to this
/// channel so `control_subscriber` can process it. If `None`, incoming messages
/// are only logged. Channel full = drop the message + log a warning (does not
/// block the eventloop — if the control subscriber is stuck, the eventloop must
/// stay alive for heartbeat/resource/telemetry publishing and auto-reconnect).
///
/// `connack`: if `Some`, every `Event::Incoming(ConnAck)` sends a `()` signal
/// to this channel. rumqttc defaults to `clean_session=true` — on every
/// reconnect the server forgets subscriptions, so the subscriber must
/// re-subscribe. `try_send` + drop if full: re-subscribe is idempotent, so
/// coalescing is safe.
pub async fn drive_eventloop(
    mut eventloop: EventLoop,
    inbox: Option<mpsc::Sender<Publish>>,
    connack: Option<mpsc::Sender<()>>,
    weigher_cmd: Option<mpsc::Sender<(String, Vec<u8>)>>,
) {
    info!(
        forward_publish = inbox.is_some(),
        forward_connack = connack.is_some(),
        forward_weigher_cmd = weigher_cmd.is_some(),
        "MQTT event loop driver started"
    );
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                debug!(topic = %p.topic, len = p.payload.len(), "MQTT incoming publish");
                // Topic "/cmd/" = weigher serial command (tare, zero, etc.)
                // Any other topic = control/set from MQTT → inbox
                if p.topic.contains("/cmd/") {
                    if let Some(tx) = weigher_cmd.as_ref() {
                        let _ = tx.try_send((p.topic.clone(), p.payload.to_vec()));
                    }
                } else if let Some(tx) = inbox.as_ref()
                    && let Err(e) = tx.try_send(p)
                {
                    warn!("control inbox full or closed, dropping message: {e}");
                }
            }
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                info!(?ack, "MQTT connected (ConnAck received)");
                if let Some(tx) = connack.as_ref() {
                    // try_send: if it's full, the subscriber hasn't processed
                    // the previous signal yet, so a new one adds no information
                    // (re-subscribing once is enough). Drop it silently.
                    let _ = tx.try_send(());
                }
            }
            Ok(event) => {
                debug!(?event, "MQTT event");
            }
            Err(e) => {
                warn!("MQTT poll error: {e:#}");
                tokio::time::sleep(POLL_ERROR_BACKOFF).await;
            }
        }
    }
}
