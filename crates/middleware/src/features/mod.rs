//! Feature modules. Setiap feature self-contained: punya handler, DTO
//! internal, dan tests sendiri. Bergantung pada `shared::` via trait,
//! tidak pada implementasi konkret.

pub mod command_dispatch;
pub mod heartbeat_track;
pub mod plc_status_track;
pub mod resource_track;
pub mod telemetry_ingest;
