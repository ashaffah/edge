//! Wire format untuk command channel: payload masuk + ack keluar.

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CommandPayload {
    pub value: serde_json::Value,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AckPayload<'a> {
    pub request_id: &'a str,
    pub status: &'a str, // "ok" | "skipped" | "error"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
