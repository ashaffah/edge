//! Modbus client abstraction: `ReadConn`/`WriteConn` traits + a factory for the
//! 3 transports (TCP / RTU-over-TCP / RTU-Serial).
//!
//! The TCP path uses `ModbusReader` + `ModbusWriter` as two separate
//! connections (a 2-client pattern). RTU half-duplex cannot be emulated with two
//! separate connections to the same physical port → it needs a single
//! connection that serializes requests. Solution: an actor task that owns the
//! connection while Reader/Writer tasks send requests over an mpsc channel.
//!
//! Two layers:
//!
//! 1. **Traits** [`ReadConn`] + [`WriteConn`] = a uniform API used by
//!    [`crate::telemetry`] and [`crate::control_subscriber`]. They don't care
//!    which transport is behind it.
//!
//! 2. **Factories** [`ReaderFactory`] + [`WriterFactory`] = built once at
//!    startup ([`build_io`]). The `.connect()` method returns a
//!    `Box<dyn ReadConn>` or `Box<dyn WriteConn>`. Per transport:
//!    - **TCP**: each `.connect()` opens a new socket (preserves the 2-client
//!      pattern).
//!    - **RTU**: the factory holds an actor handle; `.connect()` clones the
//!      handle (cheap). Reconnecting the physical link is handled inside the
//!      actor.
//!
//! TCP lifecycle:
//! ```text
//! loop {
//!     let reader = factory.connect().await?;   // open TCP
//!     loop { reader.read_holdings(...) }       // until error
//!     // drop reader → socket close → outer loop reconnects
//! }
//! ```
//!
//! The RTU lifecycle is identical from the consumer's point of view — but
//! `.connect()` just clones the handle, and a read error = a per-request error
//! (the actor restarts in the background without the caller needing to know).

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::{Reader, Slave, Writer, tcp};
use tracing::debug;

use crate::modbus_actor::ActorHandle;
use crate::settings::Transport;

// ===========================================================================
// Trait: uniform Read / Write API
// ===========================================================================

/// Bulk read holding registers + coils. Implemented by the TCP reader and the
/// RTU actor handle. Method-level state mutation (`&mut self`) so a stateful
/// impl (TCP: socket; Actor: reply buffer) can hold the handle without interior
/// mutability.
#[async_trait]
pub trait ReadConn: Send {
    /// Read `count` holding registers starting at address 0. Empty vec if
    /// count == 0 (caller no-op).
    async fn read_holdings(&mut self, count: u16) -> Result<Vec<u16>>;

    /// Read `count` coils starting at address 0.
    async fn read_coils(&mut self, count: u16) -> Result<Vec<bool>>;

    /// FC4 Read Input Registers — targeted read from a specific `address`.
    /// Differs from `read_holdings` which always reads from 0.
    async fn read_input_registers(&mut self, address: u16, count: u16) -> Result<Vec<u16>>;

    /// FC2 Read Discrete Inputs — targeted read from a specific `address`.
    /// Like `read_coils` but from the input address space (read-only).
    async fn read_discrete_inputs(&mut self, address: u16, count: u16) -> Result<Vec<bool>>;
}

/// Single-register and single-coil writes. Function codes 5 & 6 in the Modbus
/// spec — enough for mixed-mode control (holding-register on/off writes + 2 coil
/// writes per hydraulic actuator).
///
/// `write_multiple_registers` (function code 16) was added for set_* writes that
/// span 2 registers (float → 2 reg f32, integer → 2 reg i32). Used by
/// control_subscriber to dispatch set/* topics to the PLC + source devices.
#[async_trait]
pub trait WriteConn: Send {
    async fn write_single_register(&mut self, address: u16, value: u16) -> Result<()>;
    async fn write_single_coil(&mut self, address: u16, value: bool) -> Result<()>;
    /// Write N consecutive registers starting at `address`. The caller ensures
    /// `values.len()` matches the encoding (e.g. 2 for f32/i32 encoded via
    /// `ByteOrder::encode_f32` / `encode_u32`).
    async fn write_multiple_registers(&mut self, address: u16, values: Vec<u16>) -> Result<()>;
}

// ===========================================================================
// TCP: ModbusReader + ModbusWriter (preserve existing 2-client pattern)
// ===========================================================================

/// Stateful Modbus TCP reader. Connect-per-session: on disconnect/error, drop
/// and recreate it via [`ModbusReader::connect`].
pub struct ModbusReader {
    ctx: Context,
}

impl ModbusReader {
    /// Connect to the PLC via Modbus TCP with a given unit ID.
    pub async fn connect(addr: SocketAddr, unit_id: u8) -> Result<Self> {
        let ctx = tcp::connect_slave(addr, Slave(unit_id))
            .await
            .with_context(|| format!("modbus tcp connect {addr} (unit={unit_id})"))?;
        debug!(%addr, unit_id, "modbus tcp reader connected");
        Ok(Self { ctx })
    }
}

#[async_trait]
impl ReadConn for ModbusReader {
    async fn read_holdings(&mut self, count: u16) -> Result<Vec<u16>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        // tokio-modbus 0.16+ separates a transport error (outer Result) from a
        // Modbus protocol exception (inner Result, e.g. ILLEGAL_ADDRESS from the
        // PLC). Both = fatal in the poll cycle, the handler reconnects.
        let res = self
            .ctx
            .read_holding_registers(0, count)
            .await
            .context("modbus read_holding_registers transport error")?;
        let regs = res.map_err(|e| anyhow!("modbus protocol exception (holdings): {e:?}"))?;
        Ok(regs)
    }

    async fn read_coils(&mut self, count: u16) -> Result<Vec<bool>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let res = self
            .ctx
            .read_coils(0, count)
            .await
            .context("modbus read_coils transport error")?;
        let coils = res.map_err(|e| anyhow!("modbus protocol exception (coils): {e:?}"))?;
        Ok(coils)
    }

    async fn read_input_registers(&mut self, address: u16, count: u16) -> Result<Vec<u16>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let res = self
            .ctx
            .read_input_registers(address, count)
            .await
            .context("modbus read_input_registers transport error")?;
        let regs =
            res.map_err(|e| anyhow!("modbus protocol exception (input_registers): {e:?}"))?;
        Ok(regs)
    }

    async fn read_discrete_inputs(&mut self, address: u16, count: u16) -> Result<Vec<bool>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let res = self
            .ctx
            .read_discrete_inputs(address, count)
            .await
            .context("modbus read_discrete_inputs transport error")?;
        let bits =
            res.map_err(|e| anyhow!("modbus protocol exception (discrete_inputs): {e:?}"))?;
        Ok(bits)
    }
}

/// Stateful Modbus TCP writer. A connection separate from [`ModbusReader`] —
/// the 2-client pattern so telemetry reads aren't disturbed by control writes.
/// For RTU this lifecycle collapses to a single actor (see
/// [`crate::modbus_actor`]).
pub struct ModbusWriter {
    ctx: Context,
}

impl ModbusWriter {
    pub async fn connect(addr: SocketAddr, unit_id: u8) -> Result<Self> {
        let ctx = tcp::connect_slave(addr, Slave(unit_id))
            .await
            .with_context(|| format!("modbus tcp connect (writer) {addr} (unit={unit_id})"))?;
        debug!(%addr, unit_id, "modbus tcp writer connected");
        Ok(Self { ctx })
    }
}

#[async_trait]
impl WriteConn for ModbusWriter {
    async fn write_single_register(&mut self, address: u16, value: u16) -> Result<()> {
        let res = self
            .ctx
            .write_single_register(address, value)
            .await
            .with_context(|| {
                format!("modbus write_single_register(addr={address}) transport error")
            })?;
        res.map_err(|e| {
            anyhow!("modbus protocol exception (write register {address}={value}): {e:?}")
        })?;
        debug!(address, value, "modbus register written");
        Ok(())
    }

    async fn write_single_coil(&mut self, address: u16, value: bool) -> Result<()> {
        let res = self
            .ctx
            .write_single_coil(address, value)
            .await
            .with_context(|| format!("modbus write_single_coil(addr={address}) transport error"))?;
        res.map_err(|e| anyhow!("modbus protocol exception (write coil {address}): {e:?}"))?;
        debug!(address, value, "modbus coil written");
        Ok(())
    }

    async fn write_multiple_registers(&mut self, address: u16, values: Vec<u16>) -> Result<()> {
        let count = values.len();
        let res = self
            .ctx
            .write_multiple_registers(address, &values)
            .await
            .with_context(|| {
                format!(
                    "modbus write_multiple_registers(addr={address}, count={count}) transport error"
                )
            })?;
        res.map_err(|e| {
            anyhow!("modbus protocol exception (write multi {address}, {count} regs): {e:?}")
        })?;
        debug!(address, count, "modbus multi registers written");
        Ok(())
    }
}

// ===========================================================================
// Factory layer: ReaderFactory / WriterFactory
// ===========================================================================

/// Factory for creating a `ReadConn` session. For TCP, each `connect()` opens a
/// new socket. For RTU, it returns a clone of the actor handle (shared with the
/// `WriterFactory` on the same transport).
#[derive(Clone)]
pub struct ReaderFactory(Backend);

/// Factory for creating a `WriteConn` session.
#[derive(Clone)]
pub struct WriterFactory(Backend);

#[derive(Clone)]
enum Backend {
    /// TCP: descriptor only, the connection happens on each `.connect()` call.
    Tcp { addr: SocketAddr, unit_id: u8 },
    /// RTU (over-TCP or serial): a handle to the actor task. Cheap to clone.
    Actor(ActorHandle),
}

impl ReaderFactory {
    pub async fn connect(&self) -> Result<Box<dyn ReadConn>> {
        match &self.0 {
            Backend::Tcp { addr, unit_id } => {
                let r = ModbusReader::connect(*addr, *unit_id).await?;
                Ok(Box::new(r))
            }
            Backend::Actor(h) => Ok(Box::new(h.clone())),
        }
    }

    /// Derive a new factory for a different slave/unit ID (e.g. PLC unit 1
    /// → chiller unit 2). For TCP: clone the descriptor with the new unit_id
    /// (each `.connect()` opens a separate TCP socket with the MBAP unit field
    /// = this unit_id). For RTU/Actor: clone the handle with the new slave_id
    /// (sharing the same actor task). Cheap in both cases.
    pub fn with_unit_id(&self, unit_id: u8) -> Self {
        let backend = match &self.0 {
            Backend::Tcp { addr, .. } => Backend::Tcp {
                addr: *addr,
                unit_id,
            },
            Backend::Actor(h) => Backend::Actor(h.with_slave_id(unit_id)),
        };
        Self(backend)
    }

    /// Create a new TCP factory to a different host:port — for a source device
    /// that has its own IP (a second PLC, etc.). Independent of the main
    /// transport.
    pub fn for_tcp(addr: std::net::SocketAddr, unit_id: u8) -> Self {
        Self(Backend::Tcp { addr, unit_id })
    }
}

impl WriterFactory {
    pub async fn connect(&self) -> Result<Box<dyn WriteConn>> {
        match &self.0 {
            Backend::Tcp { addr, unit_id } => {
                let w = ModbusWriter::connect(*addr, *unit_id).await?;
                Ok(Box::new(w))
            }
            Backend::Actor(h) => Ok(Box::new(h.clone())),
        }
    }

    /// See [`ReaderFactory::with_unit_id`].
    pub fn with_unit_id(&self, unit_id: u8) -> Self {
        let backend = match &self.0 {
            Backend::Tcp { addr, .. } => Backend::Tcp {
                addr: *addr,
                unit_id,
            },
            Backend::Actor(h) => Backend::Actor(h.with_slave_id(unit_id)),
        };
        Self(backend)
    }

    /// See [`ReaderFactory::for_tcp`].
    pub fn for_tcp(addr: std::net::SocketAddr, unit_id: u8) -> Self {
        Self(Backend::Tcp { addr, unit_id })
    }
}

// ===========================================================================
// RTU-serial actor cache — shared across reader + writer + slaves on the same port
// ===========================================================================

/// A cache of RTU-serial actors keyed by the serial port `path`.
///
/// **Why it's needed:** `tokio_serial::SerialStream::open` opens the serial port
/// with `TIOCEXCL` (Linux). A second open to the same path is immediately
/// rejected by the OS with `"Unable to acquire exclusive lock on serial port"`.
/// Edge-client
/// has several code paths that each need Modbus access to a slave on a serial
/// port: (a) `build_io` for the main PLC if its transport is RTU-serial,
/// (b) `build_source_reader` per non-PLC slave for telemetry, (c)
/// `build_source_writer` per non-PLC slave for control. Without a cache,
/// (a) + (b) + (c) each `spawn` their own actor trying to open the same port →
/// only one wins the exclusive lock, the rest are stuck in a retry loop forever.
///
/// This cache ensures one `path` = one actor task = one `open()` call.
/// Different slave-ids on the same RS-485 bus are handled via
/// [`ActorHandle::with_slave_id`] — a cheap sender clone + changing the ID per
/// request. The sequential FIFO of the actor's mpsc channel already guarantees
/// no two Modbus frames overlap on the bus (see the `modbus_actor` module docs).
///
/// Cheap to clone (internal `Arc`). Usage pattern: create one instance at
/// startup in `main`, then pass `&SerialActorCache` to every call site that
/// needs RTU-serial access.
#[derive(Default, Clone)]
pub struct SerialActorCache {
    inner: Arc<Mutex<HashMap<String, ActorHandle>>>,
}

impl SerialActorCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up or spawn an actor for `Transport::RtuSerial`. Cache key = `path`.
    /// A repeated call with the same path reuses the actor; only the slave_id is
    /// updated in the returned handle.
    ///
    /// `timings` is used only on the first spawn for that path. Subsequent calls
    /// for the same path reuse the existing actor regardless of the new
    /// `timings`. All slaves currently use the fixed default from
    /// `device_conn_to_transport`, so there is no inconsistency to warn about.
    ///
    /// Error: `Err` only if `transport` is not `RtuSerial` (a programmer bug,
    /// not a runtime fault) or if `modbus_actor::spawn` rejects the transport.
    /// Serial I/O itself is not attempted here — `spawn` returns quickly and the
    /// actor task connects in the background with retry-on-fail.
    async fn get_or_spawn_serial(
        &self,
        transport: Transport,
        slave_id: u8,
        timings: crate::modbus_actor::RtuTimings,
    ) -> Result<ActorHandle> {
        let path = match &transport {
            Transport::RtuSerial { path, .. } => path.clone(),
            other => {
                return Err(anyhow!(
                    "SerialActorCache::get_or_spawn_serial called with a non-serial transport \
                     ({}) — programmer bug",
                    other.display()
                ));
            }
        };
        let mut lock = self.inner.lock().await;
        if let Some(existing) = lock.get(&path) {
            debug!(
                path = %path,
                slave_id,
                "RTU serial actor reused from cache"
            );
            return Ok(existing.with_slave_id(slave_id));
        }
        debug!(
            path = %path,
            slave_id,
            transport = %transport.display(),
            "RTU serial actor: cache miss, spawning new actor"
        );
        let handle = crate::modbus_actor::spawn(transport, slave_id, timings)
            .await
            .context("spawn RTU serial actor (via SerialActorCache)")?;
        lock.insert(path, handle.clone());
        Ok(handle)
    }

    /// Number of RTU-serial actor entries in the cache. Used by tests + can be
    /// used for observability in the startup log (number of physical serial
    /// ports in use).
    #[allow(dead_code)]
    pub(crate) async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }
}

// ===========================================================================
// Main builder
// ===========================================================================

/// Build a factory pair from [`ModbusSettings`]. For RTU mode, this
/// **spawns an actor task** — the RTU process keeps running in the background
/// until both factories drop (all handles drop). For TCP, no I/O happens here,
/// it just packages the descriptor.
///
/// `serial_cache` is only used if `settings.transport` is `RtuSerial`; it is
/// registered so `build_source_reader` / `build_source_writer` for slaves on the
/// same port can reuse the actor.
pub async fn build_io(
    settings: &crate::settings::ModbusSettings,
    serial_cache: &SerialActorCache,
) -> Result<(ReaderFactory, WriterFactory)> {
    match &settings.transport {
        Transport::Tcp { host, port } => {
            let addr: SocketAddr = format!("{host}:{port}")
                .parse()
                .map_err(|e| anyhow!("TCP address invalid: {e}"))?;
            let backend = Backend::Tcp {
                addr,
                unit_id: settings.unit_id,
            };
            Ok((ReaderFactory(backend.clone()), WriterFactory(backend)))
        }
        Transport::RtuOverTcp { .. } => {
            // RTU-over-TCP is not cached via SerialActorCache: it's not a
            // serial port, so the exclusive lock is irrelevant. Spawn one actor
            // + share it via a cloned handle (reader & writer use the same
            // handle). RTU-serial slaves added via `build_source_reader` /
            // `_writer` still go through the cache — different transport,
            // different actor.
            let handle = crate::modbus_actor::spawn(
                settings.transport.clone(),
                settings.unit_id,
                settings.rtu_timings,
            )
            .await?;
            Ok((
                ReaderFactory(Backend::Actor(handle.clone())),
                WriterFactory(Backend::Actor(handle)),
            ))
        }
        Transport::RtuSerial { .. } => {
            let handle = serial_cache
                .get_or_spawn_serial(
                    settings.transport.clone(),
                    settings.unit_id,
                    settings.rtu_timings,
                )
                .await
                .context("build_io: spawn RTU serial actor for the main PLC")?;
            Ok((
                ReaderFactory(Backend::Actor(handle.clone())),
                WriterFactory(Backend::Actor(handle)),
            ))
        }
    }
}

/// Build a ReaderFactory for one slave device. Used by telemetry polling.
///
/// 2 cases:
/// - TCP slave → a TCP factory to the slave host (independent of the PLC).
/// - RTU-serial slave → look up the actor in `serial_cache` (keyed by path).
///   On a cache hit (e.g. the main PLC uses the same serial port, or another
///   slave was registered earlier, or this slave's writer counterpart is already
///   registered), reuse the handle with `with_slave_id`. On a miss, spawn a new
///   actor and cache it so other call sites (the same slave's writer, or other
///   slaves on the same RS-485 bus) reuse it too.
pub(crate) async fn build_source_reader(
    conn: &crate::shared::DeviceConnection,
    serial_cache: &SerialActorCache,
) -> anyhow::Result<ReaderFactory> {
    use crate::shared::DeviceConnection;
    match conn {
        DeviceConnection::Tcp { host, port, .. } => {
            let addr: std::net::SocketAddr = format!("{host}:{port}")
                .parse()
                .map_err(|e| anyhow::anyhow!("slave tcp addr invalid: {e}"))?;
            Ok(ReaderFactory::for_tcp(addr, conn.unit_id()))
        }
        DeviceConnection::RtuSerial { .. } => {
            let (transport, timings) = crate::settings::device_conn_to_transport(conn)
                .context("slave device_conn_to_transport (reader)")?;
            let handle = serial_cache
                .get_or_spawn_serial(transport, conn.unit_id(), timings)
                .await
                .context("get_or_spawn RTU serial actor for slave reader")?;
            Ok(ReaderFactory(Backend::Actor(handle)))
        }
        DeviceConnection::SerialAscii { .. } => {
            anyhow::bail!(
                "build_source_reader called for SerialAscii — a weigher has no Modbus reader"
            )
        }
    }
}

/// Used by control_subscriber.
/// Logic identical to [`build_source_reader`] — look up the actor in
/// `serial_cache` so it's shared with the reader counterpart (if the same slave
/// also has a monitoring entry) and with other slaves on the same port.
pub(crate) async fn build_source_writer(
    conn: &crate::shared::DeviceConnection,
    serial_cache: &SerialActorCache,
) -> anyhow::Result<WriterFactory> {
    use crate::shared::DeviceConnection;
    match conn {
        DeviceConnection::Tcp { host, port, .. } => {
            let addr: std::net::SocketAddr = format!("{host}:{port}")
                .parse()
                .map_err(|e| anyhow::anyhow!("slave tcp addr invalid: {e}"))?;
            Ok(WriterFactory::for_tcp(addr, conn.unit_id()))
        }
        DeviceConnection::RtuSerial { .. } => {
            let (transport, timings) = crate::settings::device_conn_to_transport(conn)
                .context("slave device_conn_to_transport (writer)")?;
            let handle = serial_cache
                .get_or_spawn_serial(transport, conn.unit_id(), timings)
                .await
                .context("get_or_spawn RTU serial actor for slave writer")?;
            Ok(WriterFactory(Backend::Actor(handle)))
        }
        DeviceConnection::SerialAscii { .. } => {
            anyhow::bail!(
                "build_source_writer called for SerialAscii — a weigher has no Modbus writer"
            )
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modbus_actor::RtuTimings;
    use crate::settings::{SerialDataBits, SerialParity, SerialStopBits};

    fn serial_transport(path: &str) -> Transport {
        Transport::RtuSerial {
            path: path.to_string(),
            baud: 19200,
            parity: SerialParity::None,
            stop_bits: SerialStopBits::One,
            data_bits: SerialDataBits::Eight,
        }
    }

    /// The cache returns the same actor handle (identical channel sender) for
    /// two calls with the same serial path. The slave_id may differ; the actor
    /// task stays single. This is the key invariant that prevents the "Unable to
    /// acquire exclusive lock on serial port" bug — with the cache, one path =
    /// one `tokio_serial::SerialStream::open`.
    #[tokio::test]
    async fn serial_cache_reuses_actor_for_same_path() {
        let cache = SerialActorCache::new();
        let transport = serial_transport("/tmp/fake-ttyUSB-cache-test-a");
        let timings = RtuTimings::default();

        let reader_handle = cache
            .get_or_spawn_serial(transport.clone(), 1, timings)
            .await
            .expect("first call should spawn");
        let writer_handle = cache
            .get_or_spawn_serial(transport.clone(), 2, timings)
            .await
            .expect("second call should reuse");

        assert!(
            reader_handle.same_channel(&writer_handle),
            "reader & writer for the same port must share the actor task"
        );
        assert_eq!(cache.len().await, 1, "cache must have 1 entry per path");
    }

    /// Different serial path = different actor. The cache must not collapse two
    /// different physical ports into one actor — that would send frames onto the
    /// wrong bus.
    #[tokio::test]
    async fn serial_cache_separates_actors_per_path() {
        let cache = SerialActorCache::new();
        let timings = RtuTimings::default();

        let h_a = cache
            .get_or_spawn_serial(
                serial_transport("/tmp/fake-ttyUSB-cache-test-b"),
                1,
                timings,
            )
            .await
            .expect("spawn A");
        let h_b = cache
            .get_or_spawn_serial(
                serial_transport("/tmp/fake-ttyUSB-cache-test-c"),
                1,
                timings,
            )
            .await
            .expect("spawn B");

        assert!(
            !h_a.same_channel(&h_b),
            "different serial ports must have separate actor tasks"
        );
        assert_eq!(cache.len().await, 2);
    }

    /// Calling the cache with a non-serial transport = programmer bug, it must
    /// return Err (not panic, not silently spawn the wrong thing).
    #[tokio::test]
    async fn serial_cache_rejects_non_serial_transport() {
        let cache = SerialActorCache::new();
        let result = cache
            .get_or_spawn_serial(
                Transport::Tcp {
                    host: "127.0.0.1".to_string(),
                    port: 502,
                },
                1,
                RtuTimings::default(),
            )
            .await;
        assert!(result.is_err(), "TCP transport must be rejected");
    }
}
