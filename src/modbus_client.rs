//! Modbus client abstraction: `ReadConn`/`WriteConn` traits + a factory for the
//! 3 transports (TCP / RTU-over-TCP / RTU-Serial).
//!
//! **Every transport goes through a single actor connection** ([`crate::modbus_actor`]).
//! One task owns the link; reader + writer + bridge send requests over an mpsc
//! channel and the actor serializes them. This is required for RTU (half-duplex,
//! one physical port) and also for plain Modbus TCP, because many small PLCs /
//! Modbus-TCP gateways accept only ONE concurrent socket — a separate reader and
//! writer socket to the same host:port makes the second one fail (write returns
//! EINPROGRESS / os error 115).
//!
//! Actors are shared per endpoint via [`ActorCache`], keyed by connection
//! identity (`tcp:host:port`, `rtu-tcp:host:port`, `serial:path`). So a reader
//! and a writer to the **same** host:port share one connection, while a
//! split-port setup (e.g. `:502` read / `:503` write) keeps two independent
//! connections — one per key.
//!
//! Two layers:
//!
//! 1. **Traits** [`ReadConn`] + [`WriteConn`] = a uniform API used by
//!    [`crate::telemetry`] and [`crate::control_subscriber`]. They don't care
//!    which transport is behind it.
//!
//! 2. **Factories** [`ReaderFactory`] + [`WriterFactory`] = built once at
//!    startup ([`build_io`]). Each holds an actor handle; `.connect()` just
//!    clones the handle (cheap), returning a `Box<dyn ReadConn>` /
//!    `Box<dyn WriteConn>`. Reconnecting the physical link is handled inside the
//!    actor, so a read/write error is a per-request error the caller can retry
//!    without managing sockets itself.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
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
// Factory layer: ReaderFactory / WriterFactory
// ===========================================================================

/// Factory for creating a `ReadConn` session. Holds an actor handle; `connect()`
/// clones it (cheap). The actor owns the single shared connection.
#[derive(Clone)]
pub struct ReaderFactory(ActorHandle);

/// Factory for creating a `WriteConn` session. Holds an actor handle; `connect()`
/// clones it (cheap). Shares the same actor as the [`ReaderFactory`] when both
/// target the same endpoint (see [`ActorCache`]).
#[derive(Clone)]
pub struct WriterFactory(ActorHandle);

impl ReaderFactory {
    pub async fn connect(&self) -> Result<Box<dyn ReadConn>> {
        Ok(Box::new(self.0.clone()))
    }
}

impl WriterFactory {
    pub async fn connect(&self) -> Result<Box<dyn WriteConn>> {
        Ok(Box::new(self.0.clone()))
    }
}

// ===========================================================================
// Actor cache — one actor per endpoint, shared across reader + writer + slaves
// ===========================================================================

/// A cache of Modbus actors keyed by connection identity (`tcp:host:port`,
/// `rtu-tcp:host:port`, `serial:path`).
///
/// **Why it's needed:** several code paths each need Modbus access to the same
/// endpoint — (a) `build_io` for the main PLC, (b) `build_source_reader` per
/// device for telemetry, (c) `build_source_writer` per device for control.
/// Without a cache each would spawn its own actor and open its own connection.
/// That breaks two ways:
/// - **Serial**: `tokio_serial::SerialStream::open` takes `TIOCEXCL` (Linux) —
///   a second open to the same path is rejected ("Unable to acquire exclusive
///   lock on serial port").
/// - **TCP / RTU-over-TCP**: many PLCs / gateways accept only ONE concurrent
///   socket — a second connection to the same host:port makes writes fail with
///   EINPROGRESS (os error 115).
///
/// The cache guarantees one endpoint = one actor task = one connection. Reader
/// and writer to the same endpoint share it; different endpoints (e.g. a
/// split-port `:502` read / `:503` write setup) get independent actors. Multiple
/// slave-ids on one RS-485 bus are handled via [`ActorHandle::with_slave_id`] —
/// a cheap sender clone + per-request ID. The actor's FIFO mpsc channel
/// serializes frames so none overlap (see the `modbus_actor` module docs).
///
/// Cheap to clone (internal `Arc`). Create one instance at startup in `main` and
/// pass `&ActorCache` to every call site that needs Modbus access.
#[derive(Default, Clone)]
pub struct ActorCache {
    inner: Arc<Mutex<HashMap<String, ActorHandle>>>,
}

/// Cache key for an endpoint. Reader + writer to the same endpoint collapse to
/// one key (= one shared actor); a different host:port or path stays separate.
fn actor_cache_key(transport: &Transport) -> String {
    match transport {
        Transport::Tcp { host, port } => format!("tcp:{host}:{port}"),
        Transport::RtuOverTcp { host, port } => format!("rtu-tcp:{host}:{port}"),
        Transport::RtuSerial { path, .. } => format!("serial:{path}"),
    }
}

impl ActorCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up or spawn the actor for `transport`. A repeated call with the same
    /// endpoint reuses the actor; only the slave_id is updated in the returned
    /// handle.
    ///
    /// `timings` is used only on the first spawn for that endpoint. Subsequent
    /// calls reuse the existing actor regardless of the new `timings` — all
    /// callers currently pass the fixed default from `device_conn_to_transport`.
    ///
    /// I/O is not attempted here — `spawn` returns quickly and the actor task
    /// connects in the background with retry-on-fail.
    async fn get_or_spawn(
        &self,
        transport: Transport,
        slave_id: u8,
        timings: crate::modbus_actor::RtuTimings,
    ) -> Result<ActorHandle> {
        let key = actor_cache_key(&transport);
        let mut lock = self.inner.lock().await;
        if let Some(existing) = lock.get(&key) {
            debug!(key = %key, slave_id, "modbus actor reused from cache");
            return Ok(existing.with_slave_id(slave_id));
        }
        debug!(
            key = %key,
            slave_id,
            transport = %transport.display(),
            "modbus actor: cache miss, spawning new actor"
        );
        let handle = crate::modbus_actor::spawn(transport, slave_id, timings)
            .await
            .context("spawn modbus actor (via ActorCache)")?;
        lock.insert(key, handle.clone());
        Ok(handle)
    }

    /// Number of distinct endpoints (actor tasks) in the cache. Used by tests +
    /// can be used for observability in the startup log.
    #[allow(dead_code)]
    pub(crate) async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }
}

// ===========================================================================
// Main builder
// ===========================================================================

/// Build a factory pair from [`ModbusSettings`] for the main PLC. Gets (or
/// spawns) the shared actor for this endpoint via `cache`, so the reader and
/// writer share one connection, and any source device on the same endpoint
/// reuses it too. No I/O happens here — the actor connects in the background.
pub async fn build_io(
    settings: &crate::settings::ModbusSettings,
    cache: &ActorCache,
) -> Result<(ReaderFactory, WriterFactory)> {
    let handle = cache
        .get_or_spawn(
            settings.transport.clone(),
            settings.unit_id,
            settings.rtu_timings,
        )
        .await
        .context("build_io: spawn modbus actor for the main PLC")?;
    Ok((ReaderFactory(handle.clone()), WriterFactory(handle)))
}

/// Build a ReaderFactory for one source device. Used by telemetry polling.
///
/// Gets (or spawns) the shared actor for the device's endpoint via `cache`, so
/// it's shared with the main PLC / other devices on the same endpoint and with
/// this device's writer counterpart. A weigher (SerialAscii) has no Modbus
/// reader → error.
pub(crate) async fn build_source_reader(
    conn: &crate::shared::DeviceConnection,
    cache: &ActorCache,
) -> anyhow::Result<ReaderFactory> {
    use crate::shared::DeviceConnection;
    if let DeviceConnection::SerialAscii { .. } = conn {
        anyhow::bail!(
            "build_source_reader called for SerialAscii — a weigher has no Modbus reader"
        );
    }
    let (transport, timings) = crate::settings::device_conn_to_transport(conn)
        .context("slave device_conn_to_transport (reader)")?;
    let handle = cache
        .get_or_spawn(transport, conn.unit_id(), timings)
        .await
        .context("get_or_spawn modbus actor for source reader")?;
    Ok(ReaderFactory(handle))
}

/// Build a WriterFactory for one source device. Used by control_subscriber.
/// Logic identical to [`build_source_reader`] — shares the actor with the reader
/// counterpart (if the device also has a monitoring entry) and with other
/// devices on the same endpoint.
pub(crate) async fn build_source_writer(
    conn: &crate::shared::DeviceConnection,
    cache: &ActorCache,
) -> anyhow::Result<WriterFactory> {
    use crate::shared::DeviceConnection;
    if let DeviceConnection::SerialAscii { .. } = conn {
        anyhow::bail!(
            "build_source_writer called for SerialAscii — a weigher has no Modbus writer"
        );
    }
    let (transport, timings) = crate::settings::device_conn_to_transport(conn)
        .context("slave device_conn_to_transport (writer)")?;
    let handle = cache
        .get_or_spawn(transport, conn.unit_id(), timings)
        .await
        .context("get_or_spawn modbus actor for source writer")?;
    Ok(WriterFactory(handle))
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

    fn tcp_transport(host: &str, port: u16) -> Transport {
        Transport::Tcp {
            host: host.to_string(),
            port,
        }
    }

    /// The cache returns the same actor handle (identical channel sender) for
    /// two calls with the same serial path. The slave_id may differ; the actor
    /// task stays single. This is the key invariant that prevents the "Unable to
    /// acquire exclusive lock on serial port" bug — with the cache, one path =
    /// one `tokio_serial::SerialStream::open`.
    #[tokio::test]
    async fn cache_reuses_actor_for_same_serial_path() {
        let cache = ActorCache::new();
        let transport = serial_transport("/tmp/fake-ttyUSB-cache-test-a");
        let timings = RtuTimings::default();

        let reader_handle = cache
            .get_or_spawn(transport.clone(), 1, timings)
            .await
            .expect("first call should spawn");
        let writer_handle = cache
            .get_or_spawn(transport.clone(), 2, timings)
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
    async fn cache_separates_actors_per_serial_path() {
        let cache = ActorCache::new();
        let timings = RtuTimings::default();

        let h_a = cache
            .get_or_spawn(
                serial_transport("/tmp/fake-ttyUSB-cache-test-b"),
                1,
                timings,
            )
            .await
            .expect("spawn A");
        let h_b = cache
            .get_or_spawn(
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

    /// TCP reader + writer to the same host:port share ONE actor (= one socket).
    /// This is the fix for single-session PLCs: a separate reader + writer socket
    /// would make the writer fail with EINPROGRESS (os error 115).
    #[tokio::test]
    async fn cache_shares_actor_for_same_tcp_endpoint() {
        let cache = ActorCache::new();
        let timings = RtuTimings::default();

        let reader = cache
            .get_or_spawn(tcp_transport("192.168.10.10", 502), 1, timings)
            .await
            .expect("reader spawn");
        let writer = cache
            .get_or_spawn(tcp_transport("192.168.10.10", 502), 1, timings)
            .await
            .expect("writer reuse");

        assert!(
            reader.same_channel(&writer),
            "reader & writer on the same host:port must share one actor/connection"
        );
        assert_eq!(cache.len().await, 1);
    }

    /// Split-port setup (`:502` read / `:503` write, same host) keeps TWO
    /// independent actors — matches PLCs that expose a separate write port.
    #[tokio::test]
    async fn cache_separates_actors_per_tcp_port() {
        let cache = ActorCache::new();
        let timings = RtuTimings::default();

        let read = cache
            .get_or_spawn(tcp_transport("192.168.10.10", 502), 1, timings)
            .await
            .expect("read spawn");
        let write = cache
            .get_or_spawn(tcp_transport("192.168.10.10", 503), 1, timings)
            .await
            .expect("write spawn");

        assert!(
            !read.same_channel(&write),
            "different ports on the same host must have separate actors"
        );
        assert_eq!(cache.len().await, 2);
    }
}
