//! Actor task for RTU mode (RTU-over-TCP & RTU-Serial).
//!
//! Modbus RTU = half-duplex, one physical port. It can't be opened twice
//! (serial port: OS lock; RTU-over-TCP: the gateway converter usually doesn't
//! handle multi-client well). So we use the actor pattern:
//!
//! - **One task** that owns the connection (serial port or TcpStream).
//! - **An mpsc channel** for inbound requests from telemetry + control + bridge.
//! - **A oneshot reply** per request — the caller awaits the reply before
//!   sending the next request. Sequential by design: requests cannot overlap on
//!   the bus.
//! - **Internal reconnect**: on a connection error or request timeout, the actor
//!   backs off + reconnects, and pending requests are drained with a fast error.
//!
//! ## Sequential guarantee
//!
//! Because the channel is FIFO + the actor awaits each request until it
//! completes (or times out), requests from different tasks (telemetry, control,
//! bridge) are automatically serialized — no two frames on the bus at once.
//!
//! ## Multi-slave (chiller + PLC on one RS-485 bus)
//!
//! `ActorHandle` carries a `slave_id`. A caller that needs to target a different
//! slave uses [`ActorHandle::with_slave_id`] — cheap, just cloning the sender +
//! changing the ID. The actor calls `ctx.set_slave()` before executing and adds
//! [`RtuTimings::inter_frame_delay`] when the slave ID changes from the previous
//! request.
//!
//! ## Timeout
//!
//! Each request is bounded by [`RtuTimings::request_timeout`]. If a slave
//! doesn't respond within the limit, the actor reconnects and pending requests
//! are drained. Without this, one unresponsive slave could freeze the whole bus
//! indefinitely.

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_modbus::client::Context as ModbusContext;
use tokio_modbus::prelude::{Reader, Slave, SlaveContext, Writer, rtu};
use tracing::{debug, info, warn};

use crate::modbus_client::{ReadConn, WriteConn};
use crate::settings::{SerialDataBits, SerialParity, SerialStopBits, Transport};

/// Backoff between reconnect attempts — same as `telemetry::RECONNECT_BACKOFF`
/// and `control_subscriber::RECONNECT_BACKOFF`.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Actor inbox capacity. Telemetry (1 req/poll cycle) + control (sporadic)
/// + bridge (1 req/poll cycle per source param).
///
/// 32 is large enough for bursts, small enough to apply backpressure if the
/// actor is stuck. Full = the caller's `send().await` blocks until drained.
const INBOX_CAP: usize = 32;

/// Default timeout per request — used by [`RtuTimings::default()`].
/// 500ms is conservative for 9600 baud (worst-case frame ~30ms) + margin.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 500;

/// Default inter-frame delay — used by [`RtuTimings::default()`].
/// 10ms = 3.5 char-times at 9600 baud (~4ms) + margin for RS-485 noise.
const DEFAULT_INTER_FRAME_DELAY_MS: u64 = 10;

/// RTU timing parameters, tunable per deployment via env.
///
/// Built from env via [`crate::settings::load_modbus_settings`] and passed to
/// [`spawn`]. For TCP mode this struct is unused (the actor isn't spawned), but
/// it's still loaded so config validation runs the same for all transports.
#[derive(Debug, Clone, Copy)]
pub struct RtuTimings {
    /// Per-request time limit. If the slave doesn't respond, the actor
    /// reconnects. Env: `MODBUS_REQUEST_TIMEOUT_MS`. Default: 500ms.
    pub request_timeout: Duration,
    /// Silent interval between requests to a different slave ID on the same bus.
    /// Env: `MODBUS_INTER_FRAME_DELAY_MS`. Default: 10ms.
    pub inter_frame_delay: Duration,
}

impl Default for RtuTimings {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            inter_frame_delay: Duration::from_millis(DEFAULT_INTER_FRAME_DELAY_MS),
        }
    }
}

// ===========================================================================
// Public handle
// ===========================================================================

/// Cheap-clone handle to the actor task. Implements `ReadConn` + `WriteConn`, so
/// a caller can use it in place of `ModbusReader` / `ModbusWriter` on the RTU
/// code path.
///
/// `slave_id` determines the Modbus slave this handle targets. For multi-slave
/// on one bus (e.g. PLC slave=1 + chiller slave=2), create two handles from the
/// same actor via [`ActorHandle::with_slave_id`].
///
/// Drop all clones → channel closes → the actor task exits gracefully.
#[derive(Clone, Debug)]
pub struct ActorHandle {
    tx: mpsc::Sender<Request>,
    /// The slave ID stamped onto every request from this handle.
    slave_id: u8,
}

impl ActorHandle {
    /// Create a new handle to the same actor but with a different slave ID.
    /// Cheap — just cloning the sender + changing the ID. Used for multi-slave
    /// on one RS-485 bus (e.g. a PLC handle + a chiller handle from 1 actor).
    pub fn with_slave_id(&self, slave_id: u8) -> Self {
        Self {
            tx: self.tx.clone(),
            slave_id,
        }
    }

    /// True if two handles point to the same actor task (the same channel
    /// sender). Used by the `SerialActorCache` test to verify that two lookups
    /// to the same port return handles to the identical actor. `with_slave_id`
    /// is still "same channel" — cloned sender, different slave_id.
    #[cfg(test)]
    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }
}

/// Spawn an actor task for a given transport. Returns a ready-to-use handle.
/// The initial connect happens inside the task (async, in the background).
/// Requests that arrive before the connect finishes are queued and executed once
/// the connection is up.
///
/// An error here means the transport is fundamentally invalid (e.g. an
/// unsupported variant). Network errors don't surface — the actor handles them
/// internally.
pub async fn spawn(transport: Transport, unit_id: u8, timings: RtuTimings) -> Result<ActorHandle> {
    // Sanity check at startup so misconfig is detected quickly. The real connect
    // happens inside the actor loop (for automatic retry-on-fail).
    match &transport {
        Transport::RtuOverTcp { .. } | Transport::RtuSerial { .. } => {}
        Transport::Tcp { .. } => {
            return Err(anyhow!(
                "modbus_actor::spawn called with Transport::Tcp; this is a bug — \
                 TCP should use ModbusReader/Writer direct"
            ));
        }
    }

    let (tx, rx) = mpsc::channel(INBOX_CAP);
    tokio::spawn(run_actor(transport, timings, rx));
    Ok(ActorHandle {
        tx,
        slave_id: unit_id,
    })
}

// ===========================================================================
// Trait impls — forward to the channel
// ===========================================================================

#[async_trait]
impl ReadConn for ActorHandle {
    async fn read_holdings(&mut self, count: u16) -> Result<Vec<u16>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.dispatch(Op::ReadHoldings { count })
            .await?
            .into_holdings()
    }

    async fn read_input_registers(&mut self, address: u16, count: u16) -> Result<Vec<u16>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.dispatch(Op::ReadInputRegisters { address, count })
            .await?
            .into_holdings()
    }

    async fn read_discrete_inputs(&mut self, address: u16, count: u16) -> Result<Vec<bool>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.dispatch(Op::ReadDiscreteInputs { address, count })
            .await?
            .into_coils()
    }

    async fn read_coils(&mut self, count: u16) -> Result<Vec<bool>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.dispatch(Op::ReadCoils { count }).await?.into_coils()
    }
}

#[async_trait]
impl WriteConn for ActorHandle {
    async fn write_single_register(&mut self, address: u16, value: u16) -> Result<()> {
        self.dispatch(Op::WriteRegister { address, value })
            .await?
            .into_unit()
    }

    async fn write_single_coil(&mut self, address: u16, value: bool) -> Result<()> {
        self.dispatch(Op::WriteCoil { address, value })
            .await?
            .into_unit()
    }

    async fn write_multiple_registers(&mut self, address: u16, values: Vec<u16>) -> Result<()> {
        self.dispatch(Op::WriteMultipleRegisters { address, values })
            .await?
            .into_unit()
    }
}

impl ActorHandle {
    /// Send an op, await the reply. Error variants:
    /// - Actor task dead → "modbus actor task is gone" (channel closed).
    /// - Reply dropped before answering → the actor probably crashed mid-request.
    /// - Modbus error or timeout → forwarded from the actor.
    async fn dispatch(&self, op: Op) -> Result<Reply> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request {
                slave_id: self.slave_id,
                op,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow!("modbus actor task is gone (channel closed)"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("modbus actor dropped reply (probably crashed)"))?
    }
}

// ===========================================================================
// Internal protocol
// ===========================================================================

struct Request {
    /// The target Modbus slave. The actor calls `ctx.set_slave()` before
    /// executing if this ID differs from the previous request.
    slave_id: u8,
    op: Op,
    reply: oneshot::Sender<Result<Reply>>,
}

enum Op {
    ReadHoldings {
        count: u16,
    },
    ReadCoils {
        count: u16,
    },
    ReadInputRegisters {
        address: u16,
        count: u16,
    },
    ReadDiscreteInputs {
        address: u16,
        count: u16,
    },
    WriteRegister {
        address: u16,
        value: u16,
    },
    WriteCoil {
        address: u16,
        value: bool,
    },
    /// Write 2+ consecutive holding registers. Used for set_* writes
    /// (float→2 reg f32, integer→2 reg i32) that span 2 registers.
    WriteMultipleRegisters {
        address: u16,
        values: Vec<u16>,
    },
}

enum Reply {
    Holdings(Vec<u16>),
    Coils(Vec<bool>),
    Unit,
}

impl Reply {
    fn into_holdings(self) -> Result<Vec<u16>> {
        match self {
            Self::Holdings(v) => Ok(v),
            _ => Err(anyhow!("reply type mismatch: expected holdings")),
        }
    }
    fn into_coils(self) -> Result<Vec<bool>> {
        match self {
            Self::Coils(v) => Ok(v),
            _ => Err(anyhow!("reply type mismatch: expected coils")),
        }
    }
    fn into_unit(self) -> Result<()> {
        match self {
            Self::Unit => Ok(()),
            _ => Err(anyhow!("reply type mismatch: expected unit")),
        }
    }
}

// ===========================================================================
// Actor loop
// ===========================================================================

/// Connect + process request sequential, reconnect on error/timeout.
/// Exits when the channel closes (all handles dropped).
///
/// `unit_id` is not used as a fixed slave — each request carries its own
/// `slave_id`. The actor switches slave via `ctx.set_slave()` before each
/// request whose slave ID differs from the previous one.
async fn run_actor(transport: Transport, timings: RtuTimings, mut rx: mpsc::Receiver<Request>) {
    info!(
        transport = %transport.display(),
        request_timeout_ms = timings.request_timeout.as_millis(),
        inter_frame_delay_ms = timings.inter_frame_delay.as_millis(),
        "rtu actor starting"
    );

    // Outer loop: reconnect cycle.
    loop {
        let mut ctx = match connect(&transport).await {
            Ok(c) => {
                info!(transport = %transport.display(), "rtu actor connected");
                c
            }
            Err(e) => {
                warn!(
                    transport = %transport.display(),
                    "rtu actor connect failed: {e:#}, retry in {}s",
                    RECONNECT_BACKOFF.as_secs()
                );
                drain_with_error(&mut rx, &format!("rtu actor not connected: {e:#}")).await;
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
        };

        // Slave ID from the previous request. `None` = no request yet.
        // Used to detect a slave switch → inter-frame delay.
        let mut last_slave_id: Option<u8> = None;

        // Inner loop: one connection lifecycle. Break to the outer loop on error.
        loop {
            let Some(req) = rx.recv().await else {
                info!("rtu actor: all handles dropped, exiting");
                return;
            };

            // Switch slave if different from the previous request.
            // Inter-frame delay only on a slave change — consecutive same-slave
            // requests need no delay (no silent interval between frames to the
            // same slave as long as the master initiates).
            if last_slave_id != Some(req.slave_id) {
                if last_slave_id.is_some() {
                    tokio::time::sleep(timings.inter_frame_delay).await;
                }
                ctx.set_slave(Slave(req.slave_id));
                last_slave_id = Some(req.slave_id);
                debug!(slave_id = req.slave_id, "rtu actor switched slave");
            }

            match tokio::time::timeout(timings.request_timeout, process(&mut ctx, req.op)).await {
                Ok(Ok(reply)) => {
                    let _ = req.reply.send(Ok(reply));
                }
                Ok(Err(e)) => {
                    warn!("rtu actor op failed: {e:#}, will reconnect");
                    let _ = req.reply.send(Err(e));
                    drain_with_error(&mut rx, "rtu actor reconnecting").await;
                    break;
                }
                Err(_elapsed) => {
                    warn!(
                        slave_id = last_slave_id.unwrap_or(0),
                        timeout_ms = timings.request_timeout.as_millis(),
                        "rtu actor request timeout, will reconnect"
                    );
                    let _ = req.reply.send(Err(anyhow!(
                        "modbus request timeout after {}ms",
                        timings.request_timeout.as_millis()
                    )));
                    drain_with_error(&mut rx, "rtu actor reconnecting after timeout").await;
                    break;
                }
            }
        }

        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

/// Fail-fast every request already queued when the link goes down. Without this,
/// the caller would await a reply that won't arrive until reconnect — bad for
/// telemetry that ticks on a tight interval.
///
/// Best-effort: drain everything ready *now* (try_recv loop) then return. Does
/// not block — messages that arrive after this are handled on the next
/// iteration or rejected when reconnect fails again.
async fn drain_with_error(rx: &mut mpsc::Receiver<Request>, msg: &str) {
    while let Ok(req) = rx.try_recv() {
        let _ = req.reply.send(Err(anyhow!("{msg}")));
    }
}

async fn process(ctx: &mut ModbusContext, op: Op) -> Result<Reply> {
    match op {
        Op::ReadHoldings { count } => {
            let res = ctx
                .read_holding_registers(0, count)
                .await
                .context("rtu read_holding_registers transport error")?;
            let regs =
                res.map_err(|e| anyhow!("rtu modbus protocol exception (holdings): {e:?}"))?;
            Ok(Reply::Holdings(regs))
        }
        Op::ReadCoils { count } => {
            let res = ctx
                .read_coils(0, count)
                .await
                .context("rtu read_coils transport error")?;
            let coils = res.map_err(|e| anyhow!("rtu modbus protocol exception (coils): {e:?}"))?;
            Ok(Reply::Coils(coils))
        }
        Op::ReadInputRegisters { address, count } => {
            let res = ctx
                .read_input_registers(address, count)
                .await
                .context("rtu read_input_registers transport error")?;
            let regs =
                res.map_err(|e| anyhow!("rtu modbus protocol exception (input_registers): {e:?}"))?;
            Ok(Reply::Holdings(regs))
        }
        Op::ReadDiscreteInputs { address, count } => {
            let res = ctx
                .read_discrete_inputs(address, count)
                .await
                .context("rtu read_discrete_inputs transport error")?;
            let bits =
                res.map_err(|e| anyhow!("rtu modbus protocol exception (discrete_inputs): {e:?}"))?;
            Ok(Reply::Coils(bits))
        }
        Op::WriteRegister { address, value } => {
            let res = ctx
                .write_single_register(address, value)
                .await
                .with_context(|| format!("rtu write_single_register(addr={address}) error"))?;
            res.map_err(|e| {
                anyhow!("rtu modbus protocol exception (write register {address}={value}): {e:?}")
            })?;
            debug!(address, value, "rtu register written");
            Ok(Reply::Unit)
        }
        Op::WriteCoil { address, value } => {
            let res = ctx
                .write_single_coil(address, value)
                .await
                .with_context(|| format!("rtu write_single_coil(addr={address}) error"))?;
            res.map_err(|e| {
                anyhow!("rtu modbus protocol exception (write coil {address}): {e:?}")
            })?;
            debug!(address, value, "rtu coil written");
            Ok(Reply::Unit)
        }
        Op::WriteMultipleRegisters { address, values } => {
            let count = values.len();
            let res = ctx
                .write_multiple_registers(address, &values)
                .await
                .with_context(|| {
                    format!("rtu write_multiple_registers(addr={address}, count={count}) error")
                })?;
            res.map_err(|e| {
                anyhow!(
                    "rtu modbus protocol exception (write multi {address}, {count} regs): {e:?}"
                )
            })?;
            debug!(address, count, "rtu multi registers written");
            Ok(Reply::Unit)
        }
    }
}

// ===========================================================================
// Transport-specific connect
// ===========================================================================

/// Open the physical connection to the transport. The initial slave is not set
/// here — the actor will `set_slave()` before each request per `req.slave_id`.
/// A dummy slave 0xFF is used for `attach_slave` (the "broadcast" convention,
/// never used for an actual request).
async fn connect(transport: &Transport) -> Result<ModbusContext> {
    const DUMMY_SLAVE: Slave = Slave(0xFF);
    match transport {
        Transport::RtuOverTcp { host, port } => {
            let addr = format!("{host}:{port}");
            let stream = tokio::net::TcpStream::connect(&addr)
                .await
                .with_context(|| format!("rtu-over-tcp connect {addr}"))?;
            // Frame format = raw RTU (CRC), not MBAP. The default mode of
            // consumer converters (Moxa/USR/Waveshare in "transparent
            // transmission").
            Ok(rtu::attach_slave(stream, DUMMY_SLAVE))
        }
        Transport::RtuSerial {
            path,
            baud,
            parity,
            stop_bits,
            data_bits,
        } => {
            let builder = tokio_serial::new(path, *baud)
                .parity(map_parity(*parity))
                .stop_bits(map_stop_bits(*stop_bits))
                .data_bits(map_data_bits(*data_bits))
                .flow_control(tokio_serial::FlowControl::None);
            let stream = tokio_serial::SerialStream::open(&builder)
                .with_context(|| format!("open serial {path} @ {baud}"))?;
            // Give the USB-serial driver (FTDI, CH340, CP2102, etc.) time to
            // finish initializing before the first frame is sent. Without this
            // delay, the first write() can return EINPROGRESS (os error 115)
            // because the driver isn't ready yet — especially on a Raspberry Pi
            // with a USB adapter.
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(rtu::attach_slave(stream, DUMMY_SLAVE))
        }
        Transport::Tcp { .. } => Err(anyhow!(
            "actor connect should not be called with Tcp transport — bug in factory"
        )),
    }
}

fn map_parity(p: SerialParity) -> tokio_serial::Parity {
    match p {
        SerialParity::None => tokio_serial::Parity::None,
        SerialParity::Even => tokio_serial::Parity::Even,
        SerialParity::Odd => tokio_serial::Parity::Odd,
    }
}

fn map_stop_bits(s: SerialStopBits) -> tokio_serial::StopBits {
    match s {
        SerialStopBits::One => tokio_serial::StopBits::One,
        SerialStopBits::Two => tokio_serial::StopBits::Two,
    }
}

fn map_data_bits(d: SerialDataBits) -> tokio_serial::DataBits {
    match d {
        SerialDataBits::Seven => tokio_serial::DataBits::Seven,
        SerialDataBits::Eight => tokio_serial::DataBits::Eight,
    }
}

// ===========================================================================
// Tests — pure orchestration. Does not touch actual Modbus I/O.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity: spawn() explicitly rejects the Tcp transport. Defensive —
    /// without this, a factory bug could slip through into an actor task that
    /// immediately crashes in connect(). Better to fail in spawn() at startup.
    #[tokio::test]
    async fn spawn_rejects_tcp_transport() {
        let t = Transport::Tcp {
            host: "127.0.0.1".into(),
            port: 502,
        };
        let res = spawn(t, 1, RtuTimings::default()).await;
        assert!(res.is_err(), "spawn must reject Tcp transport");
        let msg = res.unwrap_err().to_string();
        assert!(
            msg.contains("Tcp"),
            "error message should mention Tcp, got: {msg}"
        );
    }

    /// Reply type assertion: if the actor reply variant mismatches (a bug in
    /// process()), into_holdings/coils/unit must error, not panic.
    #[test]
    fn reply_into_holdings_rejects_wrong_variant() {
        assert!(Reply::Unit.into_holdings().is_err());
        assert!(Reply::Coils(vec![]).into_holdings().is_err());
        // Happy path
        assert_eq!(
            Reply::Holdings(vec![1, 2, 3]).into_holdings().unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn reply_into_coils_rejects_wrong_variant() {
        assert!(Reply::Unit.into_coils().is_err());
        assert!(Reply::Holdings(vec![]).into_coils().is_err());
        assert_eq!(
            Reply::Coils(vec![true, false]).into_coils().unwrap(),
            vec![true, false]
        );
    }

    #[test]
    fn reply_into_unit_rejects_wrong_variant() {
        assert!(Reply::Holdings(vec![]).into_unit().is_err());
        assert!(Reply::Coils(vec![]).into_unit().is_err());
        assert!(Reply::Unit.into_unit().is_ok());
    }

    /// Read count 0 = short-circuit, never enters the channel at all. Important
    /// so telemetry that has no holding/coil binding doesn't spam the actor with
    /// no-op requests.
    #[tokio::test]
    async fn read_zero_count_short_circuits_without_actor() {
        let (tx, _rx) = mpsc::channel(1); // _rx drop — channel closed
        let mut h = ActorHandle { tx, slave_id: 1 };

        // count=0 → must return Ok(vec![]) without hitting the channel.
        let holdings = h
            .read_holdings(0)
            .await
            .expect("zero count should short-circuit");
        assert!(holdings.is_empty());

        let coils = h
            .read_coils(0)
            .await
            .expect("zero count should short-circuit");
        assert!(coils.is_empty());
    }

    /// Channel closed (actor task exited) → dispatch returns an error, not a
    /// hang. Important for graceful shutdown.
    #[tokio::test]
    async fn dispatch_errors_when_actor_gone() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx); // simulate actor task exited
        let mut h = ActorHandle { tx, slave_id: 1 };

        let err = h
            .read_holdings(1)
            .await
            .expect_err("must error when actor gone");
        assert!(
            err.to_string().contains("actor task is gone"),
            "error should mention actor gone, got: {err}"
        );
    }

    /// with_slave_id() creates a new handle with a different slave ID but a
    /// shared channel to the same actor.
    #[test]
    fn with_slave_id_shares_channel_changes_id() {
        let (tx, _rx) = mpsc::channel(1);
        let plc = ActorHandle { tx, slave_id: 1 };
        let chiller = plc.with_slave_id(2);

        assert_eq!(plc.slave_id, 1);
        assert_eq!(chiller.slave_id, 2);
        // Both handles point to the same channel (sender strong_count = 2).
        assert_eq!(plc.tx.strong_count(), chiller.tx.strong_count());
    }

    /// Requests from different handles carry their own slave_id.
    /// The receiving actor must be able to distinguish them.
    #[tokio::test]
    async fn request_carries_correct_slave_id() {
        let (tx, mut rx) = mpsc::channel(4);
        let plc = ActorHandle { tx, slave_id: 1 };
        let chiller = plc.with_slave_id(2);

        // Send a request from the PLC handle without waiting for a reply (no
        // actor). Just make sure the slave_id entering the channel is correct.
        let (reply_tx1, _) = oneshot::channel();
        plc.tx
            .send(Request {
                slave_id: plc.slave_id,
                op: Op::ReadHoldings { count: 1 },
                reply: reply_tx1,
            })
            .await
            .unwrap();

        let (reply_tx2, _) = oneshot::channel();
        chiller
            .tx
            .send(Request {
                slave_id: chiller.slave_id,
                op: Op::ReadHoldings { count: 1 },
                reply: reply_tx2,
            })
            .await
            .unwrap();

        let req1 = rx.recv().await.unwrap();
        let req2 = rx.recv().await.unwrap();
        assert_eq!(req1.slave_id, 1, "PLC handle must stamp slave_id=1");
        assert_eq!(req2.slave_id, 2, "Chiller handle must stamp slave_id=2");
    }

    /// The default timeout must be reasonable: not too small (instant fail)
    /// and not larger than RECONNECT_BACKOFF.
    #[test]
    fn request_timeout_default_is_reasonable() {
        let t = RtuTimings::default();
        assert!(t.request_timeout.as_millis() > 50, "timeout too small");
        assert!(
            t.request_timeout < RECONNECT_BACKOFF,
            "timeout must be smaller than the reconnect backoff"
        );
    }

    /// The default inter-frame delay must be smaller than the request timeout.
    #[test]
    fn inter_frame_delay_less_than_request_timeout() {
        let t = RtuTimings::default();
        assert!(
            t.inter_frame_delay < t.request_timeout,
            "inter-frame delay ({:?}) must be < request timeout ({:?})",
            t.inter_frame_delay,
            t.request_timeout
        );
    }
}
