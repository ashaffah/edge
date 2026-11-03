//! iot-middleware: bridge between MQTT and ScyllaDB.
//!
//! Layout:
//!   - `shared/`: infrastructure (settings, storage trait and impl, MQTT publisher, cache).
//!   - `features/`: business logic, one module per feature (telemetry_ingest, command_dispatch, ...).
//!
//! Feature services are built in `main.rs`, which passes the shared
//! infrastructure in as `Arc<dyn Trait>`.

pub mod features;
pub mod shared;
