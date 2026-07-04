# edge

Rust edge agent for industrial IoT gateways. It polls **Modbus** (TCP / RTU) from
PLCs and reads **serial-ASCII weighing scales**, then publishes each parameter to
**MQTT** — one topic per value. Everything about a machine (IDs, connections,
registers, poll interval) lives in a single JSON mapping file; there is no
per-machine recompile.

Designed to run on small ARM/x86 Linux boards (Raspberry Pi and similar) as a
long-lived systemd service.

> 🇮🇩 Indonesia: [README.id.md](README.id.md)

## Contents

- [Layout](#layout)
- [Quick start](#quick-start)
- [Configuration (`.env`)](#configuration-env)
- [Mapping file](#mapping-file)
  - [Device types & connections](#device-types--connections)
  - [Parameters & categories](#parameters--categories)
  - [Modbus bindings](#modbus-bindings)
  - [Byte order](#byte-order)
  - [Weigher parser & commands](#weigher-parser--commands)
  - [Bridge rules](#bridge-rules)
- [MQTT topic layout](#mqtt-topic-layout)
- [Control gate](#control-gate)
- [Diagnosing a scale (`scale`)](#diagnosing-a-scale-scale)
- [Cross-compilation](#cross-compilation)
- [License](#license)

## Layout

Single package `edge-client` (source under [`src/`](src/)):

| Path                           | What it is                                                                 |
| ------------------------------ | -------------------------------------------------------------------------- |
| [`src/`](src/)                 | The agent: Modbus poll + weigher read → MQTT publish, control gate, health |
| [`src/shared/`](src/shared/)   | Mapping/Modbus domain types (JSON mapping schema + register semantics)     |
| [`src/scale.rs`](src/scale.rs) | `edge-client scale` subcommand — diagnose raw scale serial output          |

One `edge-client` instance runs **one machine** (though that machine may span a
primary PLC plus additional Modbus slaves and weighers on the same board). It
does no persistence — everything is published to MQTT and consumed elsewhere.

Internally the agent spawns concurrent tasks via `tokio::select!`: the MQTT
event-loop driver, the heartbeat/resource/telemetry/PLC-status/weigher
publishers, and the control subscriber. Modbus RTU access is serialized through
a per-port actor task so a single physical serial port is opened exactly once.

## Quick start

```bash
# 1. Build
cargo build --release

# 2. Configure
cp deploy/edge-client/.env.example-edge-client .env       # broker credentials
cp example-mapping.json ./mapping.json                     # devices & registers
# edit .env (MQTT host/user/pass) and mapping.json (PLC IP, serial path, registers)

# 3. Run
MQTT_MAPPING_PATH=./mapping.json ./target/release/edge-client
```

Ready-made examples live at the repo root: `example-mapping.json`,
`example-mapping-plc-only.json`, `example-mapping-weigher-only.json`,
`example-mapping-plc-slave-weigher.json`.

## Configuration (`.env`)

Machine configuration lives in the JSON mapping. The `.env` file only holds
external connection credentials and a few runtime knobs.

| Variable              | Default          | Description                                                        |
| --------------------- | ---------------- | ------------------------------------------------------------------ |
| `MQTT_HOST`           | _(required)_     | Broker host (or full host for ws/wss)                              |
| `MQTT_PORT`           | _(required)_     | Broker port (e.g. 1883, 8883, 443)                                 |
| `MQTT_USERNAME`       | _(required)_     | Broker username                                                    |
| `MQTT_PASSWORD`       | _(required)_     | Broker password                                                    |
| `MQTT_CLIENT_ID`      | _(required)_     | Unique client id                                                   |
| `MQTT_PROTOCOL`       | `mqtt`           | `mqtt` (TCP), `mqtts` (TLS), `ws`, `wss`                           |
| `MQTT_WEBSOCKET_PATH` | `/mqtt`          | WebSocket path (ws/wss only)                                       |
| `MQTT_KEEPALIVE`      | `60`             | Keepalive seconds; the LWT fires after this timeout on a hard stop |
| `MQTT_PUBLISH_QOS`    | `1`              | Publish QoS: `0`, `1`, or `2`                                      |
| `MQTT_MAPPING_PATH`   | `./mapping.json` | Path to the JSON mapping                                           |
| `CACHE_URL`           | _(empty)_        | Valkey/Redis URL for the control gate; empty = deny all control    |
| `CONTROL_GATE_KEY`    | `control`        | Valkey hash key for the control gate                               |
| `RUST_LOG`            | `info`           | Tracing filter (e.g. `info`, `edge_client=debug`)                  |

## Mapping file

A single JSON document describes every device on the machine. The agent derives
each MQTT topic as `{base_topic}/{location}/{name}/{topic}`.

```jsonc
{
  "base_topic": "acme/site",
  "poll_interval_ms": 1000,
  "devices": [
    /* ... */
  ],
  "bridge": [
    /* ... */
  ], // optional
}
```

The **machine id** is derived from the first device, in priority order
PLC → slave → weigher, as `{location}/{name}`.

### Device types & connections

Each entry in `devices[]` has a `device_type`, a stable `device_id` (used for
routing and bridge references), a `location`, a `name`, and a `connection`.

| `device_type` | Role                                                                   |
| ------------- | ---------------------------------------------------------------------- |
| `master`      | Primary PLC — sets the machine id and the main Modbus transport        |
| `slave`       | Additional Modbus device (RS-485 chiller, sensor panel, second PLC, …) |
| `weigher`     | Serial-ASCII weighing scale (no Modbus)                                |

Connection types (`connection.type`):

```jsonc
// Modbus TCP
{ "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 }

// Modbus RTU over a serial port (USB-RS485, etc.)
{ "type": "rtu_serial", "unit_id": 8, "path": "/dev/ttyUSB0",
  "baud": 19200, "parity": "none", "stop_bits": 1, "data_bits": 8 }

// Serial ASCII (weigher scales)
{ "type": "serial_ascii", "path": "COM9",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 7 }
```

Multiple slaves on the same RS-485 bus (same `path`) share one actor task and are
distinguished by `unit_id` — the port is opened exactly once. To reach an
RTU device behind an Ethernet↔RS-485 converter running in transparent (raw-RTU)
mode, point the connection at the converter.

### Parameters & categories

Each device declares parameters under three arrays:

- **`monitoring[]`** — read and published as telemetry.
- **`set[]`** — writable setpoints (float/integer), written on MQTT publish. **Not** gate-protected.
- **`control[]`** — actuator on/off (register or coil), written on MQTT publish. **Gate-protected.**

A parameter looks like:

```jsonc
{
  "key": "temp_tank", // unique key (also the weigher regex capture name)
  "label": "Main Tank Temp", // human label
  "topic": "temp/tank", // topic suffix
  "type": "float", // float | integer | boolean | string
  "unit": "C", // optional
  "modbus": {
    /* binding — see below; omit for weigher params */
  },
}
```

A parameter without a `modbus` binding is not polled by Modbus (used for weigher
capture-group values).

### Modbus bindings

`modbus.kind` maps directly to a Modbus function code — no inference:

| `kind`                     | FC   | Use                                                   |
| -------------------------- | ---- | ----------------------------------------------------- |
| `read_coils`               | FC1  | Bulk read boolean bits from address 0 (`monitoring`)  |
| `read_discrete_inputs`     | FC2  | Targeted single-bit read (`monitoring`)               |
| `read_holding_registers`   | FC3  | Bulk read float/integer from address 0 (`monitoring`) |
| `read_input_registers`     | FC4  | Targeted read at a sparse address (`monitoring`)      |
| `write_single_coil`        | FC5  | Write 1 boolean bit (`control`)                       |
| `write_single_register`    | FC6  | Write 1 register with an on/off value (`control`)     |
| `write_multiple_registers` | FC16 | Write 2 registers (float32/int32 setpoint) (`set`)    |

Examples:

```jsonc
// FC3 monitoring float, big/little byte order
{ "kind": "read_holding_registers", "address": 10, "byte_order": "big_little" }

// FC4 with a scale multiplier (e.g. raw 250 → 25.0)
{ "kind": "read_input_registers", "address": 1000, "scale": 0.1 }

// FC5 control coil
{ "kind": "write_single_coil", "address": 4 }

// FC6 control register: value 1 on true, 3 on false
{ "kind": "write_single_register", "address": 6, "on_value": 1, "off_value": 3 }

// FC16 setpoint (2 registers)
{ "kind": "write_multiple_registers", "address": 26, "byte_order": "big_big" }
```

- `read_holding_registers` and `read_input_registers` decode a 32-bit value from
  2 registers. With `scale`, `read_input_registers` instead reads 1 register and
  multiplies.
- `off_value` defaults to `0` when omitted.

### Byte order

32-bit values span two 16-bit registers; vendors differ on packing order:

| `byte_order`    | Byte | Word | Typical vendor                   |
| --------------- | ---- | ---- | -------------------------------- |
| `big_big`       | BE   | BE   | "network order" — Schneider, ABB |
| `little_big`    | LE   | BE   | Siemens S7                       |
| `big_little`    | BE   | LE   | Schneider Quantum, some Modicon  |
| `little_little` | LE   | LE   | some Mitsubishi / generic        |

Defaults to `big_big` when omitted. If a value decodes as garbage, try swapping
byte and/or word order.

### Weigher parser & commands

A `weigher` device reads ASCII lines from serial and parses them with a regex
using **named capture groups**; each group name must match a `monitoring` `key`.

```jsonc
"parser": {
  "regex": "[A-Za-z-]*(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
  "byte_map": [ { "from": 176, "to": 48 } /* 0xB0→'0' … 0xB9→'9' */ ],
  "raw_topic": "raw"      // optional: also publish all groups as one JSON object
}
```

- `byte_map` remaps raw bytes before ASCII decode — for brands that encode digits
  as non-standard bytes (e.g. GSC uses `0xB0`–`0xB9` for `'0'`–`'9'`). Leave it
  empty for standard-ASCII brands (e.g. Fujitsu).
- `raw_topic`, if set, publishes `{"weight": 12.50, "unit": "kg"}` to
  `{base}/{location}/{name}/{raw_topic}` alongside the per-key topics.

Optional `commands[]` let an operator drive the scale over serial from MQTT.
Publishing to `{base}/{location}/{name}/cmd/{key}` writes the command's
`serial_cmd` bytes to the port (escapes: `\r \n \t \\ \xNN`):

```jsonc
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

### Bridge rules

Optional `bridge[]` rules copy a value read from one device into a setpoint on
another, every poll cycle. Bridge writes bypass the control gate (they are
automated, not operator-driven).

```jsonc
"bridge": [
  {
    "read_from": { "device": "slave1", "key": "temp_out_chiller" },
    "write_to":  { "device": "plc1",   "key": "set_temp_tank" },
    "transform": "passthrough"
  }
]
```

## MQTT topic layout

All topics are prefixed `{base_topic}/{location}/{name}` (the machine id):

| Topic               | Direction | Payload                                         |
| ------------------- | --------- | ----------------------------------------------- |
| `.../{param.topic}` | publish   | Raw scalar string, one per monitoring param     |
| `.../heartbeat`     | publish   | `{"status":"connected"}` every 1s (retained)    |
| `.../resources`     | publish   | `{"memory_pct","cpu_pct","ip"}` every 1s        |
| `.../plc/status`    | publish   | `{"status":"connected"\|"disconnected"}`        |
| `.../control/{key}` | subscribe | `"1"`/`"0"`/`"true"`/`"false"` → actuator write |
| `.../set/{key}`     | subscribe | Numeric string → setpoint write                 |
| `.../cmd/{key}`     | subscribe | Any payload → send the weigher `serial_cmd`     |

The broker Last-Will-Testament publishes `{"status":"disconnected"}` (retained)
to the primary machine's heartbeat topic if the agent dies or the network drops.
After a broker reconnect the agent re-subscribes automatically (rumqttc runs a
clean session).

## Control gate

Writes from `control/{key}` (FC5/FC6 actuators) are authorized by a Valkey/Redis
hash before each write. `set/{key}` and bridge writes bypass the gate.

A write is granted **only if** both the global flag and the per-machine flag are
`"1"`:

```sh
redis-cli HSET control global 1
redis-cli HSET control acme/site/area2/machine_c 1
```

If `CACHE_URL` is empty or Valkey is unreachable, **all control is denied**
(fail-safe). The per-machine field name is `{base_topic}/{machine_id}` — the same
identifier as the MQTT topic prefix.

## Diagnosing a scale (`scale`)

Before writing a weigher parser, use the built-in subcommand to inspect the raw
serial output (correct baud rate, encoding, line terminator):

```bash
edge-client scale                          # pick a port interactively
edge-client scale --port COM3 --baud 4800  # a specific port/baud
edge-client scale --port COM3 --scan       # try common baud rates (3s each)
edge-client scale --port COM3 --send "T\r\n"  # send tare before reading
edge-client scale --list                   # list serial ports and exit
```

Each line prints `HEX | printable-ASCII`. Readable text means the baud is right;
dots (`.`) where digits should be usually mean a non-ASCII encoding you can fix
with a `byte_map` in the mapping.

## Cross-compilation

`build.sh` builds `edge-client` for a target inside a container and drops the
binary into `bin/`:

```bash
bash build.sh armv7-musl        # Raspberry Pi OS 32-bit, static (recommended)
bash build.sh armv7-glibc       # Raspberry Pi OS 32-bit, dynamic glibc
bash build.sh aarch64           # Raspberry Pi OS 64-bit / ARM64
bash build.sh x86_64            # Linux x86_64, dynamic glibc
bash build.sh x86_64-musl       # Linux x86_64, static
bash build.sh win-x86_64-gnu    # Windows x86_64 (mingw-w64)
bash build.sh win-x86_64-msvc   # Windows x86_64 (MSVC ABI via cargo-xwin)
```

Run `bash build.sh` with no arguments to list all targets.

## License

Licensed under the [MIT License](LICENSE).
