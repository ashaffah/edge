//! Shared infrastructure middleware: settings, storage trait+impl, mqtt
//! publisher trait+impl, cache trait+impl, time helpers. Tidak ada business
//! logic di sini.
//!
//! Catatan: type `Mapping`/`TopicIndex`/`DataType` dipindah ke crate
//! `iot-middleware-shared` (workspace-level) karena shared dengan `edge-client`.

pub mod cache;
pub mod mqtt_client;
pub mod settings;
pub mod storage;
pub mod time;
