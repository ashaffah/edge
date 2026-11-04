//! Storage abstraction.
//!
//! Features use `dyn StorageRepo`. Production injects `ScyllaStorage`,
//! feature tests inject `MockStorage`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, error, info, warn};

use super::settings::ScyllaSettings;
use super::time::cql_date_from_ms;

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TelemetryRow {
    pub machine: String,
    pub key: String,
    pub ts_ms: i64,
    pub value: f64,
    pub unit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SetpointRow {
    pub machine: String,
    pub key: String,
    pub ts_ms: i64,
    pub value: f64,
    pub unit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ActuatorRow {
    pub machine: String,
    pub key: String,
    pub ts_ms: i64,
    pub state: bool,
}

#[derive(Debug, Clone)]
pub struct HeartbeatRow {
    pub machine: String,
    /// Usually `"connected"` or `"disconnected"` (LWT). Stored as-is so a new
    /// edge-client state needs no schema change.
    pub status: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone)]
pub struct PlcStatusRow {
    pub machine: String,
    /// Edge to PLC Modbus link status, usually `"connected"` or
    /// `"disconnected"`. Stored as-is so a new edge-client state needs no
    /// schema change.
    pub status: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ResourceRow {
    pub machine: String,
    pub ts_ms: i64,
    /// Edge memory use in percent (0-100, not range-checked).
    pub memory_pct: f64,
    /// Edge CPU use in percent.
    pub cpu_pct: f64,
    /// Local IP of the edge node. Empty if the network was down when it published.
    pub ip: String,
}

// ---------------------------------------------------------------------------
// Trait used by the features
// ---------------------------------------------------------------------------

/// Persistence interface. Production uses ScyllaStorage, tests use a mock.
#[async_trait]
pub trait StorageRepo: Send + Sync {
    /// Record telemetry. The production impl is non-blocking (buffered).
    fn record_telemetry(&self, row: TelemetryRow);

    /// Record a setpoint command forwarded to the edge (`setpoint_history`).
    fn record_setpoint_history(&self, row: SetpointRow);

    /// Record a setpoint value read back from the PLC (`setpoint_current`).
    fn record_setpoint_current(&self, row: SetpointRow);

    /// Record an actuator command forwarded to the edge (`actuator_history`).
    fn record_actuator_history(&self, row: ActuatorRow);

    /// Record an actuator state read back from the PLC (`actuator_current`).
    fn record_actuator_current(&self, row: ActuatorRow);

    /// Record a heartbeat (writes history and current). Fire-and-forget like
    /// setpoint/actuator: the caller does not wait on Scylla.
    fn record_heartbeat(&self, row: HeartbeatRow);

    /// Record the PLC Modbus link status from edge-client (writes history and
    /// current). Fire-and-forget. Heartbeat covers edge to MQTT, this covers
    /// edge to PLC.
    fn record_plc_status(&self, row: PlcStatusRow);

    /// Record a CPU/memory/IP snapshot of an edge node (writes history and
    /// current). Fire-and-forget.
    fn record_resource(&self, row: ResourceRow);

    /// Read the current setpoint for (machine, key) with its `ts_ms`.
    /// None if it was never written.
    async fn read_setpoint_current(&self, machine: &str, key: &str) -> Result<Option<(f64, i64)>>;

    /// Read the current actuator state with its `ts_ms`. None if it was never written.
    async fn read_actuator_current(&self, machine: &str, key: &str) -> Result<Option<(bool, i64)>>;
}

// ---------------------------------------------------------------------------
// ScyllaStorage: production impl
// ---------------------------------------------------------------------------

/// Translates any peer address gossiped by ScyllaDB to the configured seed
/// address. Required when running ScyllaDB in Docker: the node broadcasts its
/// container-internal IP which is unreachable from the host.
struct FixedAddressTranslator {
    seed: SocketAddr,
}

#[async_trait]
impl AddressTranslator for FixedAddressTranslator {
    async fn translate_address(
        &self,
        _peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.seed)
    }
}

struct Prepared {
    insert_telemetry: PreparedStatement,
    insert_setpoint_history: PreparedStatement,
    insert_setpoint_current: PreparedStatement,
    insert_actuator_history: PreparedStatement,
    insert_actuator_current: PreparedStatement,
    insert_heartbeat_history: PreparedStatement,
    insert_heartbeat_current: PreparedStatement,
    insert_plc_status_history: PreparedStatement,
    insert_plc_status_current: PreparedStatement,
    insert_resource_history: PreparedStatement,
    insert_resource_current: PreparedStatement,
    select_setpoint_current: PreparedStatement,
    select_actuator_current: PreparedStatement,
}

impl Prepared {
    async fn build(session: &Session) -> Result<Self> {
        Ok(Self {
            insert_telemetry: session
                .prepare(
                    "INSERT INTO telemetry (machine, key, day, ts, value, unit) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_telemetry")?,
            insert_setpoint_history: session
                .prepare(
                    "INSERT INTO setpoint_history (machine, key, ts, value, unit) \
                     VALUES (?, ?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_setpoint_history")?,
            insert_setpoint_current: session
                .prepare(
                    "INSERT INTO setpoint_current (machine, key, ts, value, unit) \
                     VALUES (?, ?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_setpoint_current")?,
            insert_actuator_history: session
                .prepare(
                    "INSERT INTO actuator_history (machine, key, ts, state) \
                     VALUES (?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_actuator_history")?,
            insert_actuator_current: session
                .prepare(
                    "INSERT INTO actuator_current (machine, key, ts, state) \
                     VALUES (?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_actuator_current")?,
            insert_heartbeat_history: session
                .prepare(
                    "INSERT INTO heartbeat_history (machine, ts, status) \
                     VALUES (?, ?, ?)",
                )
                .await
                .context("prepare insert_heartbeat_history")?,
            insert_heartbeat_current: session
                .prepare(
                    "INSERT INTO heartbeat_current (machine, ts, status) \
                     VALUES (?, ?, ?)",
                )
                .await
                .context("prepare insert_heartbeat_current")?,
            insert_plc_status_history: session
                .prepare(
                    "INSERT INTO plc_status_history (machine, ts, status) \
                     VALUES (?, ?, ?)",
                )
                .await
                .context("prepare insert_plc_status_history")?,
            insert_plc_status_current: session
                .prepare(
                    "INSERT INTO plc_status_current (machine, ts, status) \
                     VALUES (?, ?, ?)",
                )
                .await
                .context("prepare insert_plc_status_current")?,
            insert_resource_history: session
                .prepare(
                    "INSERT INTO resource_history (machine, ts, memory_pct, cpu_pct, ip) \
                     VALUES (?, ?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_resource_history")?,
            insert_resource_current: session
                .prepare(
                    "INSERT INTO resource_current (machine, ts, memory_pct, cpu_pct, ip) \
                     VALUES (?, ?, ?, ?, ?)",
                )
                .await
                .context("prepare insert_resource_current")?,
            select_setpoint_current: session
                .prepare(
                    "SELECT value, ts FROM setpoint_current \
                     WHERE machine = ? AND key = ?",
                )
                .await
                .context("prepare select_setpoint_current")?,
            select_actuator_current: session
                .prepare(
                    "SELECT state, ts FROM actuator_current \
                     WHERE machine = ? AND key = ?",
                )
                .await
                .context("prepare select_actuator_current")?,
        })
    }
}

fn parse_consistency(s: &str) -> Result<Consistency> {
    match s {
        "Any" => Ok(Consistency::Any),
        "One" => Ok(Consistency::One),
        "Two" => Ok(Consistency::Two),
        "Three" => Ok(Consistency::Three),
        "Quorum" => Ok(Consistency::Quorum),
        "All" => Ok(Consistency::All),
        "LocalQuorum" => Ok(Consistency::LocalQuorum),
        "EachQuorum" => Ok(Consistency::EachQuorum),
        "LocalOne" => Ok(Consistency::LocalOne),
        other => anyhow::bail!(
            "unknown SCYLLA_CONSISTENCY '{other}'. \
             Valid: Any|One|Two|Three|Quorum|All|LocalQuorum|EachQuorum|LocalOne"
        ),
    }
}

/// Maximum concurrent tasks for each group of non-telemetry writes. When a
/// group is full, the write is dropped with a warning, the same policy as
/// cache.rs: staying available matters more than completeness while Scylla is
/// overloaded.
///
/// `*_current` (readback state) has its own group, so a burst of readback
/// cannot crowd out heartbeat, PLC status, resource and history rows, which the
/// outage and audit history depend on. edge-client publishes the readback of
/// every `set`/`control` key once per poll cycle (default 1 s), and all edges
/// share this group, so size it above the total number of such keys across
/// edges. `*_current` is still written every cycle because command_dispatch
/// only trusts a state younger than `STATE_MAX_AGE_MS` (5 s).
const STATE_WRITE_CONCURRENCY: usize = 128;
const EVENT_WRITE_CONCURRENCY: usize = 32;

pub struct ScyllaStorage {
    session: Arc<Session>,
    prepared: Arc<Prepared>,
    telemetry_tx: mpsc::Sender<TelemetryRow>,
    /// Bound concurrent non-telemetry write tasks so they do not pile up while
    /// Scylla is slow: `*_current` writes, and everything else.
    state_sem: Arc<Semaphore>,
    event_sem: Arc<Semaphore>,
}

impl ScyllaStorage {
    pub async fn connect(settings: ScyllaSettings) -> Result<Self> {
        let mut builder = SessionBuilder::new();
        for node in &settings.nodes {
            builder = builder.known_node(node);
        }
        if let (Some(u), Some(p)) = (&settings.username, &settings.password) {
            builder = builder.user(u, p);
        }

        // When ScyllaDB runs in Docker it gossips its container-internal IP.
        // Translate all peer addresses to the first configured seed so the
        // driver doesn't try to reach an unreachable internal address.
        if let Some(first_node) = settings.nodes.first() {
            let seed: SocketAddr = first_node
                .parse()
                .with_context(|| format!("parse seed node address: {first_node}"))?;
            builder = builder.address_translator(Arc::new(FixedAddressTranslator { seed }));
        }

        // Default consistency LocalQuorum requires at least 2 replicas. For a single-node
        // setup (dev/Docker) use LocalOne so writes/reads succeed with 1 node alive.
        let consistency = parse_consistency(&settings.consistency)?;
        let profile = ExecutionProfile::builder().consistency(consistency).build();
        builder = builder.default_execution_profile_handle(profile.into_handle());

        let session = builder.build().await.context("connect Scylla session")?;
        session
            .use_keyspace(&settings.keyspace, false)
            .await
            .with_context(|| format!("USE keyspace {}", settings.keyspace))?;

        info!(nodes = ?settings.nodes, keyspace = %settings.keyspace, "Scylla connected");

        let prepared = Arc::new(Prepared::build(&session).await?);
        let session = Arc::new(session);

        let (tx, rx) = mpsc::channel::<TelemetryRow>(settings.telemetry_channel_capacity);
        let worker_concurrency = Arc::new(Semaphore::new(settings.telemetry_concurrency));

        tokio::spawn(telemetry_worker(
            rx,
            session.clone(),
            prepared.clone(),
            worker_concurrency,
            settings.telemetry_batch_max_rows,
            settings.telemetry_flush_interval,
        ));

        Ok(Self {
            session,
            prepared,
            telemetry_tx: tx,
            state_sem: Arc::new(Semaphore::new(STATE_WRITE_CONCURRENCY)),
            event_sem: Arc::new(Semaphore::new(EVENT_WRITE_CONCURRENCY)),
        })
    }

    /// Fire-and-forget write bounded by `sem`. When it is full (Scylla slow), the
    /// write is dropped with a warning instead of blocking the caller.
    fn spawn_write(
        sem: &Arc<Semaphore>,
        capacity: usize,
        label: &'static str,
        fut: impl std::future::Future<Output = Result<()>> + Send + 'static,
    ) {
        let Ok(permit) = Arc::clone(sem).try_acquire_owned() else {
            warn!("storage write slots full ({capacity}), dropping {label}");
            return;
        };
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = fut.await {
                error!("storage {label} write failed: {e:#}");
            }
        });
    }

    /// Event write (history, heartbeat, PLC status, resource).
    fn spawn_event(
        &self,
        label: &'static str,
        fut: impl std::future::Future<Output = Result<()>> + Send + 'static,
    ) {
        Self::spawn_write(&self.event_sem, EVENT_WRITE_CONCURRENCY, label, fut);
    }

    /// `*_current` write (readback state).
    fn spawn_state(
        &self,
        label: &'static str,
        fut: impl std::future::Future<Output = Result<()>> + Send + 'static,
    ) {
        Self::spawn_write(&self.state_sem, STATE_WRITE_CONCURRENCY, label, fut);
    }
}

#[async_trait]
impl StorageRepo for ScyllaStorage {
    fn record_telemetry(&self, row: TelemetryRow) {
        if let Err(e) = self.telemetry_tx.try_send(row) {
            match e {
                mpsc::error::TrySendError::Full(r) => warn!(
                    machine = %r.machine, key = %r.key,
                    "telemetry channel full, dropping row"
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    error!("telemetry channel closed (worker dead)")
                }
            }
        }
    }

    fn record_setpoint_history(&self, row: SetpointRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_event("setpoint_history", async move {
            insert_setpoint(&session, &prepared.insert_setpoint_history, &row).await
        });
    }

    fn record_setpoint_current(&self, row: SetpointRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_state("setpoint_current", async move {
            insert_setpoint(&session, &prepared.insert_setpoint_current, &row).await
        });
    }

    fn record_actuator_history(&self, row: ActuatorRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_event("actuator_history", async move {
            insert_actuator(&session, &prepared.insert_actuator_history, &row).await
        });
    }

    fn record_actuator_current(&self, row: ActuatorRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_state("actuator_current", async move {
            insert_actuator(&session, &prepared.insert_actuator_current, &row).await
        });
    }

    fn record_heartbeat(&self, row: HeartbeatRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_event("heartbeat", async move {
            insert_heartbeat(&session, &prepared, &row).await
        });
    }

    fn record_plc_status(&self, row: PlcStatusRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_event("plc_status", async move {
            insert_plc_status(&session, &prepared, &row).await
        });
    }

    fn record_resource(&self, row: ResourceRow) {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        self.spawn_event("resource", async move {
            insert_resource(&session, &prepared, &row).await
        });
    }

    async fn read_setpoint_current(&self, machine: &str, key: &str) -> Result<Option<(f64, i64)>> {
        let result = self
            .session
            .execute_unpaged(&self.prepared.select_setpoint_current, (machine, key))
            .await
            .context("execute select_setpoint_current")?
            .into_rows_result()
            .context("convert to rows result")?;
        let mut iter = result
            .rows::<(f64, i64)>()
            .context("type-check setpoint_current row")?;
        Ok(iter
            .next()
            .transpose()
            .context("read setpoint_current row")?)
    }

    async fn read_actuator_current(&self, machine: &str, key: &str) -> Result<Option<(bool, i64)>> {
        let result = self
            .session
            .execute_unpaged(&self.prepared.select_actuator_current, (machine, key))
            .await
            .context("execute select_actuator_current")?
            .into_rows_result()
            .context("convert to rows result")?;
        let mut iter = result
            .rows::<(bool, i64)>()
            .context("type-check actuator_current row")?;
        Ok(iter
            .next()
            .transpose()
            .context("read actuator_current row")?)
    }
}

// ---------------------------------------------------------------------------
// Worker telemetry
// ---------------------------------------------------------------------------

async fn telemetry_worker(
    mut rx: mpsc::Receiver<TelemetryRow>,
    session: Arc<Session>,
    prepared: Arc<Prepared>,
    concurrency: Arc<Semaphore>,
    batch_max: usize,
    interval: Duration,
) {
    info!(
        batch_max,
        interval_ms = interval.as_millis() as u64,
        "telemetry worker started"
    );

    let mut buf: Vec<TelemetryRow> = Vec::with_capacity(batch_max);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(row) => {
                        buf.push(row);
                        if buf.len() >= batch_max {
                            flush_telemetry(&session, &prepared, &concurrency, &mut buf).await;
                        }
                    }
                    None => {
                        if !buf.is_empty() {
                            flush_telemetry(&session, &prepared, &concurrency, &mut buf).await;
                        }
                        info!("telemetry worker stopped (channel closed)");
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    flush_telemetry(&session, &prepared, &concurrency, &mut buf).await;
                }
            }
        }
    }
}

async fn flush_telemetry(
    session: &Arc<Session>,
    prepared: &Arc<Prepared>,
    concurrency: &Arc<Semaphore>,
    buf: &mut Vec<TelemetryRow>,
) {
    if buf.is_empty() {
        return;
    }
    let rows = std::mem::take(buf);
    let n = rows.len();
    debug!(rows = n, "flushing telemetry");

    let mut tasks = Vec::with_capacity(n);
    for row in rows {
        let session = session.clone();
        let prepared = prepared.clone();
        let sem = concurrency.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => return Err(anyhow::anyhow!("semaphore closed")),
            };
            insert_telemetry(&session, &prepared, &row).await
        }));
    }

    let mut failures = 0usize;
    for t in tasks {
        match t.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                failures += 1;
                error!("telemetry insert failed: {e:#}");
            }
            Err(join_err) => {
                failures += 1;
                error!("telemetry insert task panicked: {join_err}");
            }
        }
    }
    if failures > 0 {
        warn!(failures, rows = n, "some telemetry inserts failed");
    }
}

// ---------------------------------------------------------------------------
// Insert primitives (used by the worker and the record_* methods)
// ---------------------------------------------------------------------------

async fn insert_telemetry(
    session: &Session,
    prepared: &Prepared,
    row: &TelemetryRow,
) -> Result<()> {
    let day = cql_date_from_ms(row.ts_ms); // CqlDate: start-of-day UTC bucket
    session
        .execute_unpaged(
            &prepared.insert_telemetry,
            (&row.machine, &row.key, day, row.ts_ms, row.value, &row.unit),
        )
        .await
        .context("execute insert_telemetry")?;
    Ok(())
}

/// `stmt` is `insert_setpoint_history` or `insert_setpoint_current` (same columns).
async fn insert_setpoint(
    session: &Session,
    stmt: &PreparedStatement,
    row: &SetpointRow,
) -> Result<()> {
    session
        .execute_unpaged(
            stmt,
            (&row.machine, &row.key, row.ts_ms, row.value, &row.unit),
        )
        .await
        .context("execute insert setpoint")?;
    Ok(())
}

/// `stmt` is `insert_actuator_history` or `insert_actuator_current` (same columns).
async fn insert_actuator(
    session: &Session,
    stmt: &PreparedStatement,
    row: &ActuatorRow,
) -> Result<()> {
    session
        .execute_unpaged(stmt, (&row.machine, &row.key, row.ts_ms, row.state))
        .await
        .context("execute insert actuator")?;
    Ok(())
}

async fn insert_heartbeat(
    session: &Session,
    prepared: &Prepared,
    row: &HeartbeatRow,
) -> Result<()> {
    session
        .execute_unpaged(
            &prepared.insert_heartbeat_history,
            (&row.machine, row.ts_ms, &row.status),
        )
        .await
        .context("execute insert_heartbeat_history")?;
    session
        .execute_unpaged(
            &prepared.insert_heartbeat_current,
            (&row.machine, row.ts_ms, &row.status),
        )
        .await
        .context("execute insert_heartbeat_current")?;
    Ok(())
}

async fn insert_plc_status(
    session: &Session,
    prepared: &Prepared,
    row: &PlcStatusRow,
) -> Result<()> {
    session
        .execute_unpaged(
            &prepared.insert_plc_status_history,
            (&row.machine, row.ts_ms, &row.status),
        )
        .await
        .context("execute insert_plc_status_history")?;
    session
        .execute_unpaged(
            &prepared.insert_plc_status_current,
            (&row.machine, row.ts_ms, &row.status),
        )
        .await
        .context("execute insert_plc_status_current")?;
    Ok(())
}

async fn insert_resource(session: &Session, prepared: &Prepared, row: &ResourceRow) -> Result<()> {
    session
        .execute_unpaged(
            &prepared.insert_resource_history,
            (
                &row.machine,
                row.ts_ms,
                row.memory_pct,
                row.cpu_pct,
                &row.ip,
            ),
        )
        .await
        .context("execute insert_resource_history")?;
    session
        .execute_unpaged(
            &prepared.insert_resource_current,
            (
                &row.machine,
                row.ts_ms,
                row.memory_pct,
                row.cpu_pct,
                &row.ip,
            ),
        )
        .await
        .context("execute insert_resource_current")?;
    Ok(())
}
