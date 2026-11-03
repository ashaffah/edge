//! Feature modules. Each feature has its own handler, internal DTOs and
//! tests, and depends on `shared::` through traits, not concrete types.

pub mod command_dispatch;
pub mod heartbeat_track;
pub mod plc_status_track;
pub mod resource_track;
pub mod telemetry_ingest;
