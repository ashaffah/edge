//! iot-middleware: MQTT ↔ ScyllaDB bridge untuk vakum KGI Sier.
//!
//! Struktur:
//!   - `shared/`: infrastruktur (settings, mapping, storage trait+impl, mqtt publisher).
//!   - `features/`: business logic per fitur (telemetry_ingest, command_dispatch).
//!
//! Feature service di-konstruksi di binary entry (`main.rs`) dengan
//! dependency injection: shared infra di-pass sebagai `Arc<dyn Trait>`.

pub mod features;
pub mod shared;
