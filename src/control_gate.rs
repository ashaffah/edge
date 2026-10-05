//! Redis/Valkey-backed control gate — an authorization check performed before
//! every Modbus write so operators can enable/disable control centrally.
//!
//! Exact semantics:
//! - Before each Modbus write in [`crate::control_subscriber::dispatch`], call
//!   [`ControlGate::is_granted`].
//! - `HGETALL <hash_key>` (default `"control"`, override via the env var
//!   `CONTROL_GATE_KEY`). Empty hash → **deny**.
//! - Granted **iff** `map["global"] == "1"` **AND**
//!   `map[<machine_field>] == "1"`. Strict string equality — only the literal
//!   `"1"` grants.
//! - **Field name format**: `{base_topic}/{location}/{name}` of the target
//!   device, e.g. `acme/site/area1/machine_a`. Same as the MQTT topic prefix.
//!   Computed per entry (`ControlEntry::gate_field`) since one mapping can
//!   hold several PLCs/slaves and each is granted on its own.
//! - Operators flip flags manually via `redis-cli`:
//!   ```sh
//!   redis-cli HSET control global 1
//!   redis-cli HSET control acme/site/area1/machine_a 1
//!   redis-cli HSET control acme/site/area1/machine_b 1
//!   redis-cli HSET control acme/site/area2/machine_c 1
//!   redis-cli HSET control acme/site/area3/machine_c 1
//!   ```
//! - Connection error / URL not set → log a warning + **deny everything**.

use anyhow::{Context, Result};
use redis::{AsyncCommands, aio::ConnectionManager};
use std::collections::HashMap;
use tracing::{info, warn};

/// Default hash key in Valkey.
pub const DEFAULT_GATE_KEY: &str = "control";

/// The "global kill switch" field — `"1"` enables control for all machines,
/// any other value (or missing) denies.
pub const GLOBAL_FIELD: &str = "global";

/// Authorization gate for Modbus writes. Wraps an optional Redis connection +
/// hash key. The machine field is passed in on each [`ControlGate::is_granted`] call.
///
/// `conn = None` semantics: deny all control (Redis unavailable or URL not
/// set). Deliberately not `Option<ConnectionManager>` at the call site — the
/// caller just holds a `ControlGate` and calls `is_granted`.
pub struct ControlGate {
    conn: Option<ConnectionManager>,
    hash_key: String,
}

impl ControlGate {
    /// Connect to Redis/Valkey. If `url == None` (env `CACHE_URL` empty) → the
    /// gate is disabled (deny all, log a warning once at startup).
    /// If the connection fails → same, deny all (log an error).
    ///
    /// Does not return `Result` because Redis being down must not block
    /// edge-client startup (telemetry + heartbeat still run, only control is
    /// denied until the operator fixes Redis).
    pub async fn connect(url: Option<&str>, hash_key: String) -> Self {
        let Some(url) = url else {
            warn!(
                "control gate: CACHE_URL empty, gate DISABLED (all control \
                 denied). Set CACHE_URL=redis://host:port to enable."
            );
            return Self {
                conn: None,
                hash_key,
            };
        };
        let conn = match Self::try_connect(url).await {
            Ok(c) => Some(c),
            Err(e) => {
                warn!(
                    "control gate: connecting to Redis/Valkey failed: {e:#}. Gate \
                     DISABLED for now — all control denied until edge-client \
                     restarts."
                );
                None
            }
        };
        if conn.is_some() {
            info!(
                hash_key = %hash_key,
                "control gate active — HGETALL hash → granted iff global=1 AND {{machine}}=1"
            );
        }
        Self { conn, hash_key }
    }

    async fn try_connect(url: &str) -> Result<ConnectionManager> {
        let client = redis::Client::open(url).context("create redis client")?;
        ConnectionManager::new(client)
            .await
            .context("connect to redis (ConnectionManager)")
    }

    /// Check whether the operator has granted control for `machine_field`.
    ///
    /// Returns `true` **only** if:
    /// 1. The Redis connection is active (gate enabled), and
    /// 2. HGETALL succeeds, and
    /// 3. the hash has field `global == "1"`, and
    /// 4. the hash has field `{machine_field} == "1"`.
    ///
    /// If **any** of the above is false → returns `false` with a warning log.
    /// Does not return `Result` — the caller (`dispatch`) doesn't need to know
    /// the detailed reason, just "allowed or not".
    pub async fn is_granted(&mut self, machine_field: &str) -> bool {
        let Some(conn) = self.conn.as_mut() else {
            // Gate disabled at startup, already logged a warning — don't spam
            // per message.
            return false;
        };
        let map: HashMap<String, String> = match conn.hgetall(&self.hash_key).await {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    hash_key = %self.hash_key,
                    "control gate: HGETALL failed: {e:#}, deny"
                );
                return false;
            }
        };
        evaluate_gate(&map, machine_field)
    }
}

/// Pure function: evaluate the hash against `machine_field`. Separated from
/// `is_granted` so it can be unit-tested without Redis.
///
/// Semantics:
/// - empty hash → false
/// - `global` missing or != `"1"` → false
/// - `{machine_field}` missing or != `"1"` → false
/// - both == `"1"` → true
pub fn evaluate_gate(map: &HashMap<String, String>, machine_field: &str) -> bool {
    if map.is_empty() {
        return false;
    }
    let global_ok = map.get(GLOBAL_FIELD).is_some_and(|v| v == "1");
    let machine_ok = map.get(machine_field).is_some_and(|v| v == "1");
    global_ok && machine_ok
}

// ===========================================================================
// Tests — pure function only. The Redis-backed path needs an integration test.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Field format `{base_topic}/{location}/{name}`, built in production by
    /// `control_subscriber::build_entries_map`. Hardcoded here for readability without
    /// having to mock Settings.
    const FIELD_750: &str = "acme/site/area1/machine_a";
    const FIELD_350: &str = "acme/site/area1/machine_b";
    const FIELD_2T_L8: &str = "acme/site/area2/machine_c";
    const FIELD_2T_L9: &str = "acme/site/area3/machine_c";

    fn map_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn empty_hash_denies() {
        let m = HashMap::new();
        assert!(!evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn both_flags_one_grants() {
        let m = map_of(&[("global", "1"), (FIELD_750, "1")]);
        assert!(evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn global_zero_denies_even_if_machine_one() {
        let m = map_of(&[("global", "0"), (FIELD_750, "1")]);
        assert!(!evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn machine_zero_denies_even_if_global_one() {
        let m = map_of(&[("global", "1"), (FIELD_750, "0")]);
        assert!(!evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn global_missing_denies() {
        let m = map_of(&[(FIELD_750, "1")]);
        assert!(!evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn machine_missing_denies() {
        let m = map_of(&[("global", "1")]);
        assert!(!evaluate_gate(&m, FIELD_750));
    }

    #[test]
    fn other_machine_granted_does_not_grant_this_one() {
        // The operator enabled only Area 1 machine_a + machine_b — the Area 2 +
        // Area 3 machines must still be denied.
        let m = map_of(&[
            ("global", "1"),
            (FIELD_750, "1"),
            (FIELD_350, "1"),
            (FIELD_2T_L8, "0"),
        ]);
        assert!(evaluate_gate(&m, FIELD_750));
        assert!(evaluate_gate(&m, FIELD_350));
        assert!(!evaluate_gate(&m, FIELD_2T_L8));
        assert!(!evaluate_gate(&m, FIELD_2T_L9)); // missing entirely
    }

    #[test]
    fn non_one_string_denies() {
        // Strict equality — only the literal "1" grants.
        // "2", "true", "yes" all deny.
        for v in ["0", "2", "true", "TRUE", "yes", "on", "", " "] {
            let m = map_of(&[("global", v), (FIELD_750, "1")]);
            assert!(
                !evaluate_gate(&m, FIELD_750),
                "global={v:?} should deny (only literal \"1\" grants)"
            );
        }
    }
}
