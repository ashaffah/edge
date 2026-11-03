//! Entry point: load settings, connect ke infrastruktur, wire feature
//! services dengan dependency injection, route pesan MQTT ke service yang
//! relevan.

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use edge_shared::TopicIndex;
use iot_middleware::features::command_dispatch::CommandDispatchService;
use iot_middleware::features::heartbeat_track::HeartbeatTrackService;
use iot_middleware::features::plc_status_track::PlcStatusTrackService;
use iot_middleware::features::resource_track::ResourceTrackService;
use iot_middleware::features::telemetry_ingest::TelemetryIngestService;
use iot_middleware::shared::cache::{CacheRepo, NoOpCache, ValkeyCache};
use iot_middleware::shared::mqtt_client::{MqttPublisher, RumqttcPublisher};
use iot_middleware::shared::settings::Settings;
use iot_middleware::shared::storage::{ScyllaStorage, StorageRepo};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // -------- Settings --------
    let settings = Settings::from_env().context("load settings")?;
    let index = Arc::new(TopicIndex::build(&settings.base_topic, &settings.mapping));

    info!(
        machines = settings.machines.len(),
        topics = index.len(),
        broker = %settings.broker_uri(),
        scylla_nodes = ?settings.scylla.nodes,
        keyspace = %settings.scylla.keyspace,
        cache_enabled = settings.cache.enabled(),
        "configuration loaded"
    );

    // -------- Infra: Scylla --------
    let storage: Arc<dyn StorageRepo> = Arc::new(
        ScyllaStorage::connect(settings.scylla.clone())
            .await
            .context("connect storage")?,
    );

    // -------- Infra: Cache (Valkey/Redis) --------
    let cache: Arc<dyn CacheRepo> = if settings.cache.enabled() {
        Arc::new(
            ValkeyCache::connect(settings.cache.clone())
                .await
                .context("connect cache")?,
        )
    } else {
        warn!("CACHE_URL empty — running with NoOpCache (cache disabled)");
        Arc::new(NoOpCache)
    };

    // -------- Infra: MQTT client --------
    // Pakai MqttOptions::new + set_transport manual (bukan parse_url) karena
    // rumqttc 0.25 parse_url untuk ws/wss tidak preserve scheme → WS handshake
    // gagal (issue rumqttc#808).
    let m = &settings.mqtt;
    let ws_path = m.websocket_path.as_deref().unwrap_or("/mqtt");

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
        other => anyhow::bail!("MQTT_PROTOCOL '{other}' tidak dikenal (mqtt/mqtts/ws/wss)"),
    };

    let mut opts = MqttOptions::new(&m.client_id, &host_arg, m.port);
    opts.set_keep_alive(Duration::from_secs(m.keepalive_secs));
    opts.set_credentials(&m.username, &m.password);
    opts.set_clean_session(true);
    if let Some(t) = transport {
        opts.set_transport(t);
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 1024);

    // Helper konversi i32 QoS → rumqttc enum
    let to_qos = |q: i32| match q {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    };

    // Subscribe patterns: telemetry (per mesin) + command (global)
    let mut patterns = settings.subscribe_patterns();
    // cmd pattern = "{base_topic}/cmd/#" — sama dengan CommandDispatchService::subscribe_pattern()
    patterns.push(format!("{}/cmd/#", settings.base_topic));
    let qos_list: Vec<QoS> = patterns
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i == patterns.len() - 1 {
                to_qos(m.cmd_qos)
            } else {
                to_qos(m.qos)
            }
        })
        .collect();

    // Fungsi subscribe ulang — dipanggil saat ConnAck (initial + reconnect).
    // clean_session=true → broker buang semua subscription saat disconnect.
    let subscribe_all = {
        let patterns = patterns.clone();
        let qos_list = qos_list.clone();
        move |c: &AsyncClient| {
            let patterns = patterns.clone();
            let qos_list = qos_list.clone();
            let c = c.clone();
            tokio::spawn(async move {
                for (pat, qos) in patterns.iter().zip(qos_list.iter()) {
                    if let Err(e) = c.subscribe(pat, *qos).await {
                        warn!(%pat, "subscribe gagal: {e:#}");
                    }
                }
                info!(?patterns, "subscribed");
            });
        }
    };

    let publisher: Arc<dyn MqttPublisher> = Arc::new(RumqttcPublisher::new(client.clone()));

    // -------- Wire features --------
    let telemetry_ingest = TelemetryIngestService::new(
        &settings.base_topic,
        index.clone(),
        storage.clone(),
        cache.clone(),
    );
    let command_dispatch = CommandDispatchService::new(
        settings.base_topic.clone(),
        &index,
        storage.clone(),
        publisher.clone(),
        m.publish_qos,
    );
    let heartbeat_track = HeartbeatTrackService::new(
        &settings.base_topic,
        &settings.machines,
        storage.clone(),
        publisher,
        m.publish_qos,
    );
    let plc_status_track =
        PlcStatusTrackService::new(&settings.base_topic, &settings.machines, storage.clone());
    let resource_track =
        ResourceTrackService::new(&settings.base_topic, &settings.machines, storage.clone());

    // Background: tandai mesin offline kalau heartbeat-nya basi. Meng-cover
    // mesin sekunder (multi-PLC per instance edge-client) yang tidak punya LWT
    // sendiri — LWT hanya untuk machine_id primer tiap koneksi.
    tokio::spawn(heartbeat_track.clone().run_staleness_monitor());

    // -------- Event loop --------
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("connected to broker");
                subscribe_all(&client);
            }
            Ok(Event::Incoming(Packet::Publish(msg))) => {
                let topic = &msg.topic;
                let payload = String::from_utf8_lossy(&msg.payload);

                if command_dispatch.owns_topic(topic) {
                    command_dispatch.dispatch(topic.to_string(), msg.payload.to_vec());
                } else if heartbeat_track.owns_topic(topic) {
                    heartbeat_track.handle(topic, &payload);
                } else if plc_status_track.owns_topic(topic) {
                    plc_status_track.handle(topic, &payload);
                } else if resource_track.owns_topic(topic) {
                    resource_track.handle(topic, &payload);
                } else {
                    telemetry_ingest.handle(topic, &payload);
                }
            }
            Ok(_) => {} // PingReq, PingResp, SubAck, dll — diabaikan
            Err(e) => {
                warn!("MQTT event loop error: {e:#} — reconnect otomatis");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}
