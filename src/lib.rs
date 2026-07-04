//! Edge-client library: building blocks for an MQTT ↔ Modbus bridge.
//!
//! This crate exposes the modules used by the `main.rs` binary to connect
//! PLCs/Modbus devices to an MQTT broker. The transport to the PLC (TCP,
//! RTU-over-TCP, RTU serial) is configured per device via a JSON mapping
//! (`example-mapping.json`).
//!
//! # Architecture in brief
//!
//! The `edge-client` binary spawns concurrent tasks via `tokio::select!`:
//! the MQTT eventloop driver + publishers (heartbeat / resource / telemetry /
//! weigher) + the control subscriber. The modules below provide the
//! implementation of each task plus its dependencies (config, MQTT client,
//! Modbus I/O).
//!
//! # Public modules
//!
//! - [`settings`] — Loads configuration from `.env` + parses the JSON mapping
//!   (broker URI, `machine_id`, Modbus host/unit, per-parameter bindings by
//!   monitoring/set/control category). Entry point:
//!   [`settings::Settings::from_env`].
//! - [`mqtt`] — Builds a `rumqttc` AsyncClient + EventLoop with the transport
//!   derived automatically from `MQTT_PROTOCOL` (`mqtt`/`mqtts`/`ws`/`wss`), a
//!   configured LWT, topic builder helpers, and [`mqtt::drive_eventloop`] which
//!   forwards ConnAck + incoming Publish events to downstream consumers over
//!   channels.
//! - [`heartbeat`] — Periodic publisher (1s) to `{base}/{machine}/heartbeat`.
//!   The LWT fires after the keepalive timeout when the edge dies.
//! - [`resource`] — Periodic publisher (1s) of CPU/mem/IP to
//!   `{base}/{machine}/resources`.
//! - [`modbus_client`] — Modbus wrapper. Reader for register polling (used by
//!   [`telemetry`]) and Writer for dispatching control commands (used by
//!   [`control_subscriber`]). Mixed-mode: `write_single_register` for on/off
//!   devices, `write_single_coil` for momentary coils.
//! - [`telemetry`] — Polling loop per `poll_interval_ms`, decodes registers
//!   according to the bindings in the mapping, publishes each param to
//!   `{base}/{machine}/{topic}`. Only `Category::Monitoring` is published.
//! - [`control_subscriber`] — Subscribes to `{base}/{machine}/control/#`,
//!   receives commands from MQTT, dispatches them to the Modbus writer.
//!   Re-subscribes automatically after a broker reconnect.
//! - [`weigher`] — Reads ASCII lines from a scale's serial port, parses them
//!   via regex, publishes each key to MQTT. Optional: combined JSON to
//!   `raw_topic`.
//!
//! # Channel wiring
//!
//! Two `tokio::sync::mpsc` channels are wired between tasks (set up by
//! `main.rs`):
//! - **inbox** (cap 64) — `mqtt::drive_eventloop` → `control_subscriber`:
//!   forwards incoming Publish events.
//! - **connack** (cap 1) — `mqtt::drive_eventloop` → `control_subscriber`:
//!   ConnAck signal to trigger a re-subscribe.

pub mod control_gate;
pub mod control_subscriber;
pub mod heartbeat;
pub mod modbus_actor;
pub mod modbus_client;
pub mod mqtt;
pub mod plc_status;
pub mod resource;
pub mod scale;
pub mod settings;
pub mod shared;
pub mod telemetry;
pub mod weigher;
