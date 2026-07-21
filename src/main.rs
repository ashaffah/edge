//! Edge-client entry point.
//!
//! - Load settings from `.env`, parse the JSON mapping, init tracing.
//! - Connect MQTT (via `rumqttc`) with a configured LWT. The transport is
//!   chosen automatically from `MQTT_PROTOCOL` (`mqtt`/`mqtts`/`ws`/`wss`).
//! - Publish a periodic heartbeat every 1s to `{base}/{machine}/heartbeat`.
//! - Publish resources (CPU/mem/IP) every 1s to `{base}/{machine}/resources`.
//! - Poll Modbus per `poll_interval_ms`, decode registers according to the
//!   mapping bindings, publish each param to `{base}/{machine}/{topic}`. Only
//!   Category::Monitoring is published (control readback is not re-published).
//! - Subscribe to `{base}/{machine}/control/#`, receive commands from MQTT,
//!   dispatch them to Modbus. On/off devices → `write_single_register` with
//!   `on_value`/`off_value` from the mapping. Momentary coil →
//!   `write_single_coil`.
//! - Re-subscribe automatically after a broker reconnect — the eventloop
//!   forwards ConnAck to control_subscriber to re-subscribe.
//! - Control gate: check `HGETALL <CONTROL_GATE_KEY>` in Valkey/Redis before
//!   every Modbus write for a control command. `CACHE_URL` empty / Redis down
//!   → deny all control (fail-safe). Details in the `control_gate` module.
//! - Read weigher serial ASCII, parse via regex, publish each key to MQTT.
//!   Optional: publish combined JSON to `raw_topic` if configured.
//! - Run until `Ctrl+C`.
//!
//! End-to-end smoke test (requires an MQTT broker):
//!   1. `cp .env.example-edge-client .env`, edit MQTT_HOST + credentials.
//!      Edit `mapping.json` to match the device/PLC connections in the field.
//!   2. `cargo run`
//!   3. In another terminal, subscribe to all machine topics to watch data flow:
//!      `mosquitto_sub -h <host> -t 'acme/site/#' -v`
//!      Heartbeat, resources, and per-param telemetry should appear every
//!      second.
//!   4. Test control: publish `"1"` to `acme/site/{machine}/control/lamp`
//!      via `mosquitto_pub`. The edge log should show
//!      `"control written" key=control_lamp kind=register address=10 value=1`.
//!   5. Restart the broker while the edge is running → the log should show
//!      `"MQTT connected (ConnAck received)"` + `"control (re-)subscribed
//!      after ConnAck"` without restarting edge-client.
//!   6. `Ctrl+C` edge-client → wait ~60s (keepalive timeout) → the broker
//!      publishes the LWT `{"status":"disconnected"}` to the primary machine's
//!      heartbeat topic.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::signal;
use tokio::sync::mpsc;
use tracing::info;

use edge_client::control_gate::ControlGate;
use edge_client::control_subscriber::{self, ControlSubscriberConfig};
use edge_client::modbus_client;
use edge_client::scale::{self, ScaleArgs};
use edge_client::settings::Settings;
use edge_client::{heartbeat, mqtt, plc_status, resource, telemetry, weigher};

/// Edge agent. With no subcommand → run the agent (poll Modbus + publish MQTT).
#[derive(Parser)]
#[command(name = "edge-client", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Diagnose scale serial output (baud rate + encoding) before writing a parser.
    Scale(ScaleArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the CLI first — if there's a subcommand, handle it and exit without
    // touching agent setup (dotenv / tracing / Settings).
    let cli = Cli::parse();
    if let Some(Command::Scale(args)) = cli.command {
        scale::run(args).await;
        return Ok(());
    }

    // Load `.env` BEFORE tracing init so `RUST_LOG` in the file is read by
    // `EnvFilter`. If called after init (e.g. from `Settings::from_env`), the
    // filter is already baked with an empty env var → all logs are dropped.
    dotenvy::dotenv().ok();

    // Fall back to `info` if `RUST_LOG` is still absent (e.g. the binary is run
    // without a `.env` and without a shell export) — friendlier than total
    // silence.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let settings = Settings::from_env().context("load edge-client settings")?;

    // Count total params across all devices
    let total_params = settings
        .mapping
        .devices
        .iter()
        .map(|d| d.monitoring.len() + d.set.len() + d.control.len())
        .sum::<usize>();

    info!(
        machine_id = %settings.machine_id,
        base_topic = %settings.base_topic,
        broker = %settings.broker_uri(),
        modbus_active = settings.modbus.is_some(),
        weighers = settings.mapping.devices.iter().filter(|d| d.device_type == edge_client::shared::DeviceType::Weigher).count(),
        mapping_params = total_params,
        "edge-client configured"
    );

    // Enumerate each device at startup — so every PLC/slave/weigher is visible
    // in the log, not just the aggregate. Useful to verify multi-device parsing.
    for d in &settings.mapping.devices {
        use edge_client::shared::DeviceConnection;
        let conn = match &d.connection {
            DeviceConnection::Tcp {
                host,
                port,
                unit_id,
            } => format!("tcp://{host}:{port} unit={unit_id}"),
            DeviceConnection::RtuSerial {
                path,
                unit_id,
                baud,
                ..
            } => format!("rtu://{path}@{baud} unit={unit_id}"),
            DeviceConnection::SerialAscii { path, baud, .. } => format!("serial://{path}@{baud}"),
        };
        info!(
            device_id = %d.device_id,
            device_type = ?d.device_type,
            machine = %format!("{}/{}", d.location, d.name),
            conn = %conn,
            monitoring = d.monitoring.len(),
            set = d.set.len(),
            control = d.control.len(),
            "device configured"
        );
    }

    if let Some(modbus) = &settings.modbus {
        info!(
            modbus_transport = %modbus.transport.display(),
            modbus_unit = modbus.unit_id,
            poll_interval_ms = modbus.poll_interval.as_millis() as u64,
            "modbus settings"
        );
    }

    // ---------------------------------------------------------------------------
    // Modbus I/O — only built if there is a PLC in the mapping.
    // If there isn't, the telemetry/control/plc_status tasks use pending()
    // (idle).
    // ---------------------------------------------------------------------------
    let actor_cache = modbus_client::ActorCache::new();

    let (reader_factory_opt, writer_factory_opt) = if let Some(modbus) = &settings.modbus {
        let (rf, wf) = modbus_client::build_io(modbus, &actor_cache)
            .await
            .context("build modbus I/O")?;
        (Some(rf), Some(wf))
    } else {
        (None, None)
    };

    if let (Some(rf), Some(wf)) = (reader_factory_opt, writer_factory_opt) {
        let (plc_status_tx, plc_status_rx) = tokio::sync::watch::channel(false);

        let tc =
            edge_client::telemetry::TelemetryConfig::from_settings(&settings, rf, &actor_cache)
                .await
                .context("build telemetry config")?;
        let cc = ControlSubscriberConfig::from_settings(&settings, wf, &actor_cache)
            .await
            .context("build control subscriber config")?;

        info!(
            heartbeat_interval_secs = 1,
            resource_interval_secs = 1,
            telemetry_active = tc.entries.len(),
            telemetry_source_sessions = tc.source_sessions.len(),
            control_bindings = cc.entries.len(),
            control_patterns = ?cc.subscribe_patterns,
            "starting modbus tasks"
        );

        let (client, eventloop) = mqtt::build_client(&settings).context("build MQTT client")?;
        let heartbeat_topics = mqtt::heartbeat_topics(&settings);
        let resource_topics = mqtt::resource_topics(&settings);
        let plc_status_topic = mqtt::plc_status_topic(&settings);
        let qos = mqtt::publish_qos(&settings);

        // Per-source plc_status: one channel + publisher per source device
        // (additional PLC / slave), each with its own machine_id.
        // Senders are passed to telemetry::run (order = tc.source_sessions),
        // receivers are used by the per-machine plc_status publisher.
        let mut source_status_tx = Vec::with_capacity(tc.source_sessions.len());
        let mut source_status_pub: Vec<(String, tokio::sync::watch::Receiver<bool>)> = Vec::new();
        for session in &tc.source_sessions {
            let (tx, rx) = tokio::sync::watch::channel(false);
            source_status_tx.push(tx);
            let topic = format!("{}/{}/plc/status", settings.base_topic, session.machine_id);
            source_status_pub.push((topic, rx));
        }

        let heartbeat_client = client.clone();
        let resource_client = client.clone();
        let telemetry_client = client.clone();
        let plc_status_client = client.clone();
        let control_client = client.clone();
        let weigher_client = client.clone();

        let (inbox_tx, inbox_rx) = mpsc::channel(64);
        let (connack_tx, connack_rx) = mpsc::channel(1);

        let gate = ControlGate::connect(
            settings.control_gate.url.as_deref(),
            settings.control_gate.hash_key.clone(),
            settings.control_gate_field(),
        )
        .await;

        let weigher_entries = settings.mapping.devices.clone();
        let base_topic_str = settings.base_topic.clone();

        // Channel to forward MQTT cmd topics to the weigher serial write tasks.
        let (weigher_cmd_tx, weigher_cmd_rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>)>(32);

        let telemetry_fut = tokio::spawn(telemetry::run(
            telemetry_client,
            tc,
            qos,
            plc_status_tx,
            source_status_tx,
        ));
        let control_fut = tokio::spawn(control_subscriber::run(
            control_client,
            cc,
            inbox_rx,
            connack_rx,
            gate,
        ));
        let plc_status_fut = tokio::spawn(plc_status::run(
            plc_status_client,
            plc_status_topic,
            qos,
            plc_status_rx,
        ));
        // Per-source-device plc_status publishers (additional PLC / slave) —
        // detached; live for the lifetime of the process. Each has its own
        // machine_id.
        for (topic, rx) in source_status_pub {
            tokio::spawn(plc_status::run(client.clone(), topic, qos, rx));
        }

        tokio::select! {
            _ = mqtt::drive_eventloop(eventloop, Some(inbox_tx), Some(connack_tx), Some(weigher_cmd_tx)) => {
                info!("MQTT event loop exited (unexpected)");
            }
            _ = heartbeat::run(heartbeat_client, heartbeat_topics, qos) => {
                info!("heartbeat publisher exited (unexpected)");
            }
            _ = resource::run(resource_client, resource_topics, qos) => {
                info!("resource publisher exited (unexpected)");
            }
            _ = async { telemetry_fut.await.ok(); } => {
                info!("telemetry publisher exited (unexpected)");
            }
            _ = async { plc_status_fut.await.ok(); } => {
                info!("plc status publisher exited (unexpected)");
            }
            _ = async { control_fut.await.ok(); } => {
                info!("control subscriber exited (unexpected)");
            }
            _ = weigher::run(weigher_client, base_topic_str, weigher_entries, qos, weigher_cmd_rx) => {
                info!("weigher publisher exited (unexpected)");
            }
            result = signal::ctrl_c() => {
                result.context("install ctrl-c handler")?;
                info!("ctrl-c received, shutting down.");
            }
        }
    } else {
        // Weigher-only mode: no Modbus at all.
        // Only the heartbeat + resource + weigher tasks run.
        info!("no PLC in mapping — weigher-only mode");

        let (client, eventloop) = mqtt::build_client(&settings).context("build MQTT client")?;
        let heartbeat_topics = mqtt::heartbeat_topics(&settings);
        let resource_topics = mqtt::resource_topics(&settings);
        let qos = mqtt::publish_qos(&settings);

        let heartbeat_client = client.clone();
        let resource_client = client.clone();
        let weigher_client = client.clone();

        let weigher_entries = settings.mapping.devices.clone();
        let base_topic_str = settings.base_topic.clone();

        // Channel to forward MQTT cmd topics to the weigher serial write tasks.
        let (weigher_cmd_tx, weigher_cmd_rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>)>(32);

        // Weigher-only doesn't need inbox/connack — there's no control subscriber.
        tokio::select! {
            _ = mqtt::drive_eventloop(eventloop, None, None, Some(weigher_cmd_tx)) => {
                info!("MQTT event loop exited (unexpected)");
            }
            _ = heartbeat::run(heartbeat_client, heartbeat_topics, qos) => {
                info!("heartbeat publisher exited (unexpected)");
            }
            _ = resource::run(resource_client, resource_topics, qos) => {
                info!("resource publisher exited (unexpected)");
            }
            _ = weigher::run(weigher_client, base_topic_str, weigher_entries, qos, weigher_cmd_rx) => {
                info!("weigher publisher exited (unexpected)");
            }
            result = signal::ctrl_c() => {
                result.context("install ctrl-c handler")?;
                info!("ctrl-c received, shutting down.");
            }
        }
    }

    Ok(())
}
