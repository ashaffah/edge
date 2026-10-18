# edge

Rust edge agent for industrial IoT gateways. It polls Modbus (TCP / RTU) from
PLCs and reads serial-ASCII weighing scales, then publishes each parameter to
MQTT, one topic per value. Everything about a machine (IDs, connections,
registers, poll interval) lives in a single JSON mapping file, so a new machine
does not need a rebuild.

It runs on small ARM/x86 Linux boards (Raspberry Pi and similar) as a
long-lived systemd service.

> 🇮🇩 Indonesia: [README.id.md](README.id.md)

## Contents

- [Layout](#layout)
- [Quick start](#quick-start)
- [Configuration (`.env`)](#configuration-env)
- [Mapping file](#mapping-file)
  - [Device types & connections](#device-types--connections)
  - [Parameters & categories](#parameters--categories)
  - [Readback](#readback)
  - [Modbus bindings](#modbus-bindings)
  - [Byte order](#byte-order)
  - [Weigher parser & commands](#weigher-parser--commands)
  - [Scale notes](#scale-notes)
  - [Bridge rules](#bridge-rules)
- [MQTT topic layout](#mqtt-topic-layout)
- [Control gate](#control-gate)
- [Diagnosing a scale (`scale`)](#diagnosing-a-scale-scale)
- [Cross-compilation](#cross-compilation)
- [License](#license)

## Layout

Single package `edge-client`:

- [`src/`](src/): the agent. Modbus polling and weigher reading, MQTT publishing,
  control gate, heartbeat and resource reporting.
- [`src/shared/`](src/shared/): mapping and Modbus domain types (JSON mapping
  schema, register semantics).
- [`src/scale.rs`](src/scale.rs): the `edge-client scale` subcommand, which
  prints raw scale serial output.

One `edge-client` instance runs one machine. That machine can have a primary PLC
plus additional Modbus slaves and weighers on the same board. The agent stores
nothing locally: everything is published to MQTT and consumed elsewhere.

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

The machine id is `{location}/{name}` of the first `master` device. If there
is none, the first `slave` is used, then the first `weigher`.

### Device types & connections

Each entry in `devices[]` has a `device_type`, a stable `device_id` (used for
routing and bridge references), a `location`, a `name`, and a `connection`.

- `master`: primary PLC. Sets the machine id and the main Modbus transport.
- `slave`: additional Modbus device (RS-485 chiller, sensor panel, second PLC).
- `weigher`: serial-ASCII weighing scale, no Modbus.

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
distinguished by `unit_id`, so the port is opened once. A `tcp` connection
speaks Modbus TCP, so an Ethernet to RS-485 converter has to run as a Modbus TCP
gateway. Raw RTU over TCP (the converter's transparent mode) exists as a
transport in the code but cannot be selected from the mapping yet.

### Parameters & categories

Each device declares parameters under three arrays:

- `monitoring[]`: read and published as telemetry.
- `set[]`: writable setpoints (float/integer), written on MQTT publish.
- `control[]`: actuator on/off (register or coil), written on MQTT publish.

The array only decides the topic segment (`set/` or `control/`). Whether a
write goes through the [control gate](#control-gate) depends on `modbus.kind`,
and the agent does not check that the two match. Keep `write_multiple_registers`
in `set[]` and `write_single_coil` / `write_single_register` in `control[]`.

A parameter looks like:

```jsonc
{
  "key": "temp_tank", // unique key (also the weigher regex capture name)
  "label": "Main Tank Temp", // human label
  "topic": "temp/tank", // topic suffix
  "type": "float", // float | integer | boolean | string
  "unit": "C", // optional
  "modbus": {
    /* binding, see below; omit for weigher params */
  },
}
```

A parameter without a `modbus` binding is not polled by Modbus (used for weigher
capture-group values).

### Readback

Add `"readback": true` to a `set[]` or `control[]` entry to have the agent read
its write address back every poll cycle and publish the value to
`{base_topic}/{location}/{name}/state/{topic}` (e.g. `.../state/control/lamp`).
A consumer can use this as the PLC state, for example to skip a command whose
value is already set. It also reflects changes made from an HMI/panel, as long
as the HMI writes the same address.

- `write_single_coil`: read with FC1, published as `"true"` / `"false"`.
- `write_single_register`: read with FC3. `on_value` becomes `"true"`,
  `off_value` becomes `"false"`, any other value is not published. Two entries
  that share one register (e.g. lift up = 4, lift down = 8, off = 0) each only
  report their own value, so while the register is 4 the "down" state is not
  published.
- `write_multiple_registers`: read with FC3 using the same `byte_order` as the
  write, published as a number.

Reads are grouped by address into requests under the Modbus limits (125
registers / 2000 coils), separate from the telemetry bulk read. A device whose
readback fails is skipped for 30 s. Enable `readback` only if the PLC keeps the
written value at that address.

### Modbus bindings

`modbus.kind` maps directly to a Modbus function code:

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
| `big_big`       | BE   | BE   | Schneider, ABB ("network order") |
| `little_big`    | LE   | BE   | Siemens S7                       |
| `big_little`    | BE   | LE   | Schneider Quantum, some Modicon  |
| `little_little` | LE   | LE   | some Mitsubishi / generic        |

Defaults to `big_big` when omitted. If a value decodes as garbage, try swapping
byte and/or word order.

### Weigher parser & commands

A `weigher` device reads ASCII lines from serial and parses them with a regex
using named capture groups. Each group name must match a `monitoring` `key`.

```jsonc
"parser": {
  "regex": "[A-Za-z-]*(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
  "byte_map": [ { "from": 176, "to": 48 } /* 0xB0→'0' … 0xB9→'9' */ ],
  "raw_topic": "raw",          // optional: also publish all groups as one JSON object
  "stable": { "group": "st", "equals": "ST" }, // optional: publish only settled readings
  "read_timeout_ms": 5000       // optional: reopen the port if silent this long
}
```

- `byte_map` remaps raw bytes before ASCII decode, for brands that encode digits
  as non-standard bytes (e.g. GSC uses `0xB0`–`0xB9` for `'0'`–`'9'`). Leave it
  empty for standard-ASCII brands (e.g. Fujitsu).
- `raw_topic`, if set, publishes `{"weight": 12.50, "unit": "kg"}` to
  `{base}/{location}/{name}/{raw_topic}` alongside the per-key topics.
- `stable` gates publishing on the scale's stable/motion flag: add a capture
  group for the status token (e.g. `(?P<st>ST|US)`) and set `group`/`equals`.
  Only lines whose token equals `equals` are published, so in-motion readings are
  dropped. Omit to publish every parsed line.
- `read_timeout_ms` is an idle watchdog: if the port stays open but sends nothing
  for this long, the connection is reopened (catches a silent-but-open port).
  Omit for poll/on-demand scales that are idle between reads; set it to a few
  times the output interval for continuous scales.

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

### Scale notes

Settings used for the scales in the weighing room. All of them are `weigher`
devices with a `serial_ascii` connection and no Modbus. The `connection` blocks
differ per brand, so do not copy one to another scale without checking.

#### GSC

- Serial: 9600 baud, 7 data bits, no parity, 1 stop bit.
- Digits are sent as bytes `0xB0` to `0xB9` instead of ASCII `'0'` to `'9'`, so
  `byte_map` maps bytes 176 to 185 onto 48 to 57.
- The regex has an optional `st` group for the `ST`/`US` status token. It is
  published in the `raw` JSON, but no `stable` rule is set, so in-motion lines
  are published too.
- Commands end with CR LF: `T` tares, `Z` zeroes, `P` prints.

```jsonc
"connection": { "type": "serial_ascii", "path": "COM3",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 7 },
"parser": {
  "regex": "(?:(?P<st>ST|US)\\D*?)?(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
  "byte_map": [
    { "from": 176, "to": 48 }, { "from": 177, "to": 49 },
    { "from": 178, "to": 50 }, { "from": 179, "to": 51 },
    { "from": 180, "to": 52 }, { "from": 181, "to": 53 },
    { "from": 182, "to": 54 }, { "from": 183, "to": 55 },
    { "from": 184, "to": 56 }, { "from": 185, "to": 57 }
  ],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

#### Fujitsu (analytical balance)

- Serial: 9600 baud, 8 data bits, no parity, 1 stop bit.
- Output is standard ASCII, so `byte_map` stays empty.
- Each reading is a multi-line record with one field per line: `TTYPE`, `ID`,
  `DATE`, `TIME`, `TEMP`, `BAT`, `MODE`, `REF`, `STATUS`, `STEP`, `TARE`,
  `ZERO`, `WT`. The weight line looks like `WT:0.00g`, or `WT:-0.12g` after a
  tare. The unit is grams.
- The parser works per line, so the regex is anchored on `WT:`. Without it,
  lines such as `TEMP:25.9C`, `BAT:7.1V` or `REF:2000.00g` are captured as weight
  too. `[-+]?` is needed, otherwise negative readings do not match and are
  dropped without a warning.
- The `stable` rule cannot be used: `STATUS:STEADY` is on a different line from
  `WT:`, so the status group never matches the weight line and every line would
  be rejected. All readings are published.
- Commands end with CR LF: `T` tares, `Z` zeroes, `P` prints.

```jsonc
"connection": { "type": "serial_ascii", "path": "/dev/ttyUSB0",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "WT:\\s*(?P<weight>[-+]?[0-9.]+)\\s*(?P<unit>\\D+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

#### Radwag WLC/C2 (60 kg)

- Serial: 9600 baud, 8 data bits, no parity, 1 stop bit (Radwag RS232 default).
- Output is standard ASCII, so `byte_map` stays empty.
- The regex was checked against the scale. It is not anchored and takes the
  first number (integer or decimal) followed by a unit anywhere in the line.
- There is no `stable` rule, so in-motion lines (marked `?`, `^` or `v` by the
  scale) are published together with stable ones.
- Commands end with CR LF: `T` tares, `Z` zeroes, and `print` sends `S` (Radwag's
  "send stable result" command).

```jsonc
"connection": { "type": "serial_ascii", "path": "COM4",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "(?P<weight>[-+]?\\d+(?:\\.\\d+)?)\\s*(?P<unit>[A-Za-z]+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "S\\r\\n" }
]
```

#### Gewinn XK3118T1

- Read from the indicator's DB9 RS232 port, which is separate from the load
  cell port.
- Serial: 2400 baud, 8 data bits, no parity, 1 stop bit. The baud rate was
  confirmed with a Web Serial test tool.
- The indicator runs in communication mode `Co 3`: a continuous stream of net
  weight and unit, about 7.8 frames per second, resolution 0.02 kg, each frame
  ending with CR LF. A frame looks like `=0000.14(kg)`, a negative one like
  `=-000.06(kg)`. The regex skips the `(` between the number and the unit.
- `Co 3` frames have no stable flag, so the `stable` rule cannot be used.
  Stable detection (window plus tolerance) is not implemented.
- Commands: the XK3118T1 manual lists tare as `STX E ETX` and zero as
  `STX D ETX`, for mode `Co 5` (command-response). Tested in `Co 3`, the
  indicator does not reply and the stream does not change. `Co 5` has not been
  tested.
- The manual has no serial print command. Print is tied to the physical
  accumulate key and only outputs in mode `Co 6` when TOTAL is pressed. The
  `print` command is kept with an empty `serial_cmd` until this is confirmed on
  the device.

```jsonc
"connection": { "type": "serial_ascii", "path": "COM4",
  "baud": 2400, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "(?P<weight>[-+]?\\d+\\.\\d+)\\D*?(?P<unit>[A-Za-z]+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "\\x02E\\x03" }, // no effect in Co 3
  { "key": "zero",  "serial_cmd": "\\x02D\\x03" }, // no effect in Co 3
  { "key": "print", "serial_cmd": "" }             // not validated
]
```

### Bridge rules

**Not implemented yet.** `bridge[]` rules are parsed and their format is fixed,
but no task runs them, so nothing is copied. The plan is to copy a value read
from one device into a setpoint on another every poll cycle, without going
through the control gate.

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
| `.../control/{key}` | subscribe | `"1"`/`"0"`/`"true"`/`"false"`, actuator write  |
| `.../set/{key}`     | subscribe | Numeric string, setpoint write                  |
| `.../cmd/{key}`     | subscribe | Any payload, sends the weigher `serial_cmd`     |
| `.../state/{topic}` | publish   | Readback of `set`/`control` entries, see [Readback](#readback) |

The broker Last-Will-Testament publishes `{"status":"disconnected"}` (retained)
to the primary machine's heartbeat topic if the agent dies or the network drops.
After a broker reconnect the agent re-subscribes automatically (rumqttc runs a
clean session).

The agent does not check who published to `control/`, `set/` or `cmd/`, and it
has no dedup, ack or audit log. If you need those, put a service in front of
the agent that receives commands on its own topics and forwards them, and use
broker ACLs so that only this service can publish to the agent's `control/#`,
`set/#` and `cmd/#` topics.

## Control gate

Writes with `modbus.kind` `write_single_coil` or `write_single_register`
(FC5/FC6, the `control[]` actuators) are authorized by a Valkey/Redis hash
before each write. `write_multiple_registers` writes (FC16, the `set[]`
setpoints) and weigher `cmd/{key}` commands are not checked.

A write is granted only if both the global flag and the per-machine flag are
`"1"`:

```sh
redis-cli HSET control global 1
redis-cli HSET control acme/site/area2/machine_c 1
```

The per-machine field name is `{base_topic}/{machine_id}`, the same identifier
as the MQTT topic prefix.

The gate fails closed. **Control is denied** when:

- `CACHE_URL` is empty.
- Valkey is unreachable at startup. The agent does not retry this connection,
  so control stays denied until the agent restarts.
- `HGETALL` fails at runtime (that command is denied).
- `CONTROL_GATE_KEY` is set but empty (`CONTROL_GATE_KEY=`). The empty value is
  used as the hash key instead of the default, and a hash with an empty name
  normally has no flags.
  Remove the line to use the default `control`.

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
