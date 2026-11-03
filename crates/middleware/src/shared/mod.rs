//! Middleware infrastructure: settings, storage, MQTT publisher and cache
//! (each a trait plus its implementation), and time helpers. No business
//! logic here.
//!
//! The mapping types (`MiddlewareMapping`, `TopicIndex`, `DataType`) live in
//! the `edge-shared` crate because edge-client uses them too.

pub mod cache;
pub mod mqtt_client;
pub mod settings;
pub mod storage;
pub mod time;
