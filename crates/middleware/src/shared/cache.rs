//! Cache abstraction.
//!
//! Cache adalah **mirror, bukan source of truth**. Scylla tetap authoritative.
//! Write-through: setiap state change ditulis ke Scylla AND cache.
//! Read disediakan untuk fitur turunan (state_query HTTP API, dst).
//!
//! Implementasi production: `ValkeyCache` (wire-compatible dgn Redis & Valkey).
//! Untuk dev/CI tanpa Valkey: set `CACHE_URL=""` → `NoOpCache` aktif (no-op semua).
//! Untuk test feature: pakai `MockCache` di tests masing-masing.

use anyhow::{Context, Result};
use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use super::settings::CacheSettings;
use super::storage::{ActuatorRow, SetpointRow, TelemetryRow};

// ---------------------------------------------------------------------------
// Cached value types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCurrent {
    pub value: f64,
    pub ts_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetpointCurrent {
    pub value: f64,
    pub ts_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActuatorCurrent {
    pub state: bool,
    pub ts_ms: i64,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Cache repository — write-through mirror untuk current state.
///
/// `record_*` sync, fire-and-forget (spawn task untuk production impl).
/// Caller tidak boleh block waiting for cache write — cache bukan critical path.
///
/// `read_*` async; pemakaian oleh fitur turunan (mis. state_query HTTP).
/// Implementasi mengembalikan `Ok(None)` kalau key tidak ada (bukan error).
#[async_trait]
pub trait CacheRepo: Send + Sync {
    fn record_telemetry_current(&self, row: &TelemetryRow);
    fn record_setpoint_current(&self, row: &SetpointRow);
    fn record_actuator_current(&self, row: &ActuatorRow);

    async fn read_telemetry_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<TelemetryCurrent>>;
    async fn read_setpoint_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<SetpointCurrent>>;
    async fn read_actuator_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<ActuatorCurrent>>;
}

// ---------------------------------------------------------------------------
// ValkeyCache — production impl (wire-compatible Redis & Valkey)
// ---------------------------------------------------------------------------

/// Batas maksimum concurrent cache write task yang boleh berjalan serentak.
/// Kalau penuh → write baru di-drop (log warn). Match pattern telemetry channel
/// drop: cache bukan critical path, availability lebih penting dari completeness.
const CACHE_WRITE_CONCURRENCY: usize = 64;

pub struct ValkeyCache {
    conn: ConnectionManager,
    prefix: String,
    telemetry_ttl_secs: u64,
    state_ttl_secs: u64,
    /// Semaphore untuk bound concurrent `spawn_set` tasks. Mencegah task
    /// accumulation saat Valkey lambat/overloaded — terutama penting untuk
    /// `record_telemetry_current` yang dipanggil di hot path (1 Hz × N params).
    write_sem: Arc<Semaphore>,
}

impl ValkeyCache {
    pub async fn connect(settings: CacheSettings) -> Result<Self> {
        let client =
            redis::Client::open(settings.url.as_str()).context("create redis/valkey client")?;
        let conn = ConnectionManager::new(client)
            .await
            .context("connect to redis/valkey")?;
        info!(
            url = %redact_url(&settings.url),
            prefix = %settings.key_prefix,
            "cache connected"
        );
        Ok(Self {
            conn,
            prefix: settings.key_prefix,
            telemetry_ttl_secs: settings.telemetry_ttl_secs,
            state_ttl_secs: settings.state_ttl_secs,
            write_sem: Arc::new(Semaphore::new(CACHE_WRITE_CONCURRENCY)),
        })
    }

    fn key(&self, ns: &str, machine: &str, key: &str) -> String {
        format!("{}:{}:{}:{}", self.prefix, ns, machine, key)
    }

    /// Spawn a fire-and-forget SETEX. Bounded oleh `CACHE_WRITE_CONCURRENCY`:
    /// kalau semaphore penuh (Redis/Valkey lambat/overloaded), write baru di-drop
    /// dengan warn — sama dengan policy drop di telemetry channel. Cache bukan
    /// critical path; availability task loop lebih penting dari completeness.
    fn spawn_set(&self, key: String, payload: String, ttl_secs: u64) {
        let sem = Arc::clone(&self.write_sem);
        let Ok(permit) = sem.try_acquire_owned() else {
            warn!(%key, "cache write slots full ({CACHE_WRITE_CONCURRENCY}), dropping write");
            return;
        };
        let mut conn = self.conn.clone();
        tokio::spawn(async move {
            let _permit = permit; // drop saat task selesai → slot dibebaskan
            let res: redis::RedisResult<()> = conn.set_ex(&key, &payload, ttl_secs).await;
            if let Err(e) = res {
                warn!(%key, "cache write failed: {e:#}");
            }
        });
    }

    async fn get_json<T: for<'de> serde::Deserialize<'de>>(
        &self,
        key: String,
    ) -> Result<Option<T>> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn
            .get(&key)
            .await
            .with_context(|| format!("cache GET {key}"))?;
        match raw {
            None => Ok(None),
            Some(s) => {
                Ok(Some(serde_json::from_str::<T>(&s).with_context(|| {
                    format!("cache value invalid JSON at {key}")
                })?))
            }
        }
    }
}

#[async_trait]
impl CacheRepo for ValkeyCache {
    fn record_telemetry_current(&self, row: &TelemetryRow) {
        let payload = match serde_json::to_string(&TelemetryCurrent {
            value: row.value,
            ts_ms: row.ts_ms,
            unit: row.unit.clone(),
        }) {
            Ok(p) => p,
            Err(_) => return,
        };
        self.spawn_set(
            self.key("telemetry", &row.machine, &row.key),
            payload,
            self.telemetry_ttl_secs,
        );
    }

    fn record_setpoint_current(&self, row: &SetpointRow) {
        let payload = match serde_json::to_string(&SetpointCurrent {
            value: row.value,
            ts_ms: row.ts_ms,
            unit: row.unit.clone(),
        }) {
            Ok(p) => p,
            Err(_) => return,
        };
        self.spawn_set(
            self.key("setpoint", &row.machine, &row.key),
            payload,
            self.state_ttl_secs,
        );
    }

    fn record_actuator_current(&self, row: &ActuatorRow) {
        let payload = match serde_json::to_string(&ActuatorCurrent {
            state: row.state,
            ts_ms: row.ts_ms,
        }) {
            Ok(p) => p,
            Err(_) => return,
        };
        self.spawn_set(
            self.key("actuator", &row.machine, &row.key),
            payload,
            self.state_ttl_secs,
        );
    }

    async fn read_telemetry_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<TelemetryCurrent>> {
        self.get_json(self.key("telemetry", machine, key)).await
    }

    async fn read_setpoint_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<SetpointCurrent>> {
        self.get_json(self.key("setpoint", machine, key)).await
    }

    async fn read_actuator_current(
        &self,
        machine: &str,
        key: &str,
    ) -> Result<Option<ActuatorCurrent>> {
        self.get_json(self.key("actuator", machine, key)).await
    }
}

// ---------------------------------------------------------------------------
// NoOpCache — fallback ketika CACHE_URL kosong (dev/CI tanpa Valkey)
// ---------------------------------------------------------------------------

/// Cache yang tidak melakukan apa-apa. Aktif kalau `CACHE_URL` di env kosong.
pub struct NoOpCache;

#[async_trait]
impl CacheRepo for NoOpCache {
    fn record_telemetry_current(&self, _row: &TelemetryRow) {}
    fn record_setpoint_current(&self, _row: &SetpointRow) {}
    fn record_actuator_current(&self, _row: &ActuatorRow) {}

    async fn read_telemetry_current(
        &self,
        _machine: &str,
        _key: &str,
    ) -> Result<Option<TelemetryCurrent>> {
        Ok(None)
    }
    async fn read_setpoint_current(
        &self,
        _machine: &str,
        _key: &str,
    ) -> Result<Option<SetpointCurrent>> {
        Ok(None)
    }
    async fn read_actuator_current(
        &self,
        _machine: &str,
        _key: &str,
    ) -> Result<Option<ActuatorCurrent>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Buang password dari `redis://user:pass@host` untuk logging.
/// Manual parse (hindari nambah crate `url` cuma untuk ini).
fn redact_url(url: &str) -> String {
    // Cari skema://userinfo@host. userinfo bentuknya "user:pass" atau "user".
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = scheme_end + 3;
    let rest = &url[after_scheme..];
    let Some(at_idx) = rest.find('@') else {
        return url.to_string(); // no userinfo
    };
    let userinfo = &rest[..at_idx];
    let Some(colon_idx) = userinfo.find(':') else {
        return url.to_string(); // user only, no password
    };
    let user = &userinfo[..colon_idx];
    let after_at = &rest[at_idx..];
    format!("{}{}:***{}", &url[..after_scheme], user, after_at)
}
