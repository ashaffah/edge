//! Edge-client domain types.
//!
//! Contains the MQTT parameter mapping definitions + Modbus register semantics.
//! Pure types with no I/O — the file loader lives in `settings.rs`.

mod mapping;
mod modbus;
pub use mapping::*;
pub use modbus::*;
