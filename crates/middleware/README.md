# iot-middleware

Server-side bridge between MQTT and ScyllaDB. It stores what edge-client
publishes and sits between operators and edge-client for `set` and `control`
commands: it validates each command, skips it if the PLC already has that value,
forwards it to edge-client, records it and replies with an ack.

One instance serves every machine listed in its mapping.

## What it does

- Telemetry ingest: subscribes to `{base}/{machine}/#` and writes monitoring
  values (plain string payloads) to the `telemetry` table, plus a write-through
  copy in Valkey/Redis when a cache is configured.
- Readback: values on `{base}/{machine}/state/{topic}` (see the root README,
  [Readback](../../README.md#readback)) go to `setpoint_current` and
  `actuator_current`. These tables are only filled by readback, never by
  commands.
- Command dispatch: subscribes to `{base}/cmd/#`. See [Commands](#commands).
- Heartbeat, PLC status and resources: stores `heartbeat`, `plc/status` and
  `resources` in their `*_history` and `*_current` tables. A machine whose
  `connected` heartbeat is older than 5 seconds is marked `disconnected`, which
  also covers secondary machines that have no LWT of their own.

## Layout

```
crates/middleware/src/
├── main.rs                 wiring: load settings, connect, route messages
├── shared/
│   ├── settings.rs         env vars and mapping loader
│   ├── storage.rs          StorageRepo trait, ScyllaStorage
│   ├── mqtt_client.rs      MqttPublisher trait, RumqttcPublisher
│   └── cache.rs            CacheRepo trait, ValkeyCache / NoOpCache
└── features/
    ├── telemetry_ingest/   monitoring and readback to Scylla and cache
    ├── command_dispatch/   operator commands: validate, dedup, forward, ack
    ├── heartbeat_track/    liveness
    ├── plc_status_track/   edge to PLC link status
    └── resource_track/     CPU/memory/IP
```

## Quick start

Requirements: ScyllaDB, an MQTT broker, and optionally Valkey/Redis.

```bash
# 1. Schema (or use deploy/middleware/docker-compose.yml, which runs it)
cqlsh -f deploy/middleware/init.cql

# 2. Configure
cp deploy/middleware/.env.example-iot-middleware .env
cp -r deploy/middleware/mqtt-mapping ./mqtt-mapping
# edit .env (broker, Scylla, cache) and the mapping files

# 3. Run
cargo run --release -p iot-middleware
```

On startup the log shows `configuration loaded`, then `connected to broker` and
`subscribed` once the broker accepts the connection.

## Mapping

`MQTT_MAPPING_PATH` is a single JSON file or a directory. In a directory,
`_base.json` holds `base_topic` (and optionally `poll_interval_ms`) and every
other `*.json` file holds one machine. Files are read in name order, and two
files with the same `location` and `name` stop the middleware at startup.

[`deploy/middleware/mqtt-mapping/`](../../deploy/middleware/mqtt-mapping/)
matches the machines in `example-mapping.json`. A machine file looks like this:

```json
{
  "location": "area2",
  "name": "machine_c",
  "monitoring": [
    { "key": "temp_tank", "label": "Tank Temperature", "topic": "temp/tank", "type": "float", "unit": "C" }
  ],
  "set": [
    { "key": "set_temp_tank", "label": "Tank Temp Setpoint", "topic": "set/temp/tank", "type": "float", "unit": "C" }
  ],
  "control": [
    { "key": "control_motor", "label": "Motor On/Off", "topic": "control/motor", "type": "boolean" }
  ]
}
```

The entry fields (`key`, `label`, `topic`, `type`, `unit`) are the same as in
the edge-client mapping, and the topics must match it. Connection and `modbus`
fields are not needed here. `set` and `control` can be omitted.

## Configuration (`.env`)

| Variable                       | Default               | Description                                    |
| ------------------------------ | --------------------- | ---------------------------------------------- |
| `MQTT_HOST`                    | required              | Broker host                                    |
| `MQTT_PORT`                    | required              | Broker port                                    |
| `MQTT_USERNAME`                | required              | Broker user                                    |
| `MQTT_PASSWORD`                | required              | Broker password                                |
| `MQTT_CLIENT_ID`               | required              | Client ID, unique per instance                 |
| `MQTT_PROTOCOL`                | `mqtt`                | `mqtt`, `mqtts`, `ws` or `wss`                 |
| `MQTT_TLS_SSL`                 | `false`               | TLS on/off                                     |
| `MQTT_WEBSOCKET_PATH`          | `/mqtt`               | Path for `ws`/`wss`                            |
| `MQTT_KEEPALIVE`               | `60`                  | Keepalive in seconds                           |
| `MQTT_QOS`                     | `1`                   | QoS of the telemetry subscription              |
| `MQTT_CMD_QOS`                 | `1`                   | QoS of the command subscription                |
| `MQTT_PUBLISH_QOS`             | `1`                   | QoS of forwarded commands and acks             |
| `MQTT_MAPPING_PATH`            | `./mqtt-mapping.json` | Mapping file or directory                      |
| `SCYLLA_NODES`                 | `127.0.0.1:9042`      | Comma-separated nodes                          |
| `SCYLLA_KEYSPACE`              | `iot_middleware`      | Keyspace                                       |
| `SCYLLA_USERNAME`              | none                  | Optional                                       |
| `SCYLLA_PASSWORD`              | none                  | Optional                                       |
| `SCYLLA_CONSISTENCY`           | `LocalOne`            | Consistency level                              |
| `SCYLLA_TELEMETRY_CHANNEL`     | `10000`               | Telemetry buffer size                          |
| `SCYLLA_TELEMETRY_BATCH`       | `100`                 | Rows per telemetry batch                       |
| `SCYLLA_TELEMETRY_FLUSH_MS`    | `200`                 | Telemetry flush interval                       |
| `SCYLLA_TELEMETRY_CONCURRENCY` | `32`                  | Concurrent telemetry inserts                   |
| `CACHE_URL`                    | empty (disabled)      | e.g. `redis://localhost:6379`                  |
| `CACHE_KEY_PREFIX`             | `iot:middleware`      | Cache key prefix                               |
| `CACHE_TELEMETRY_TTL_SECS`     | `3600`                | TTL of cached telemetry                        |
| `CACHE_STATE_TTL_SECS`         | `86400`               | TTL of cached setpoint/actuator state          |

## Commands

Operators publish JSON to `{base}/cmd/{location}/{name}/set/{key}` or
`.../control/{key}`:

```bash
mosquitto_pub -t 'acme/site/cmd/area2/machine_c/control/motor' \
  -m '{"value": true, "request_id": "req-001"}'

mosquitto_pub -t 'acme/site/cmd/area2/machine_c/set/temp/tank' \
  -m '{"value": 80.5, "request_id": "req-002"}'
```

For each command the middleware:

1. Rejects it if the topic is not in the mapping.
2. Checks the value against the entry `type`. Control takes `true`/`false`,
   `0`/`1` or the strings `"true"`/`"false"`/`"on"`/`"off"`/`"1"`/`"0"`. Set
   takes a number, and an `integer` entry rejects fractions.
3. Compares it with `setpoint_current` / `actuator_current`. If the readback is
   at most 5 seconds old and equal, it acks `"skipped"` and stops.
4. Publishes a plain string to the topic without `/cmd/`
   (`acme/site/area2/machine_c/control/motor` with `"1"`), which edge-client
   subscribes to.
5. Writes the command to `setpoint_history` / `actuator_history`.
6. Acks on `{base}/ack/{request_id}` if a `request_id` was given. A rejected
   command acks `"error"` with the reason in `detail`. Invalid JSON gets no ack.

For the two commands above the acks would look like this (an illustration of
the format, not captured output):

```
acme/site/ack/req-001 {"request_id":"req-001","status":"ok"}
acme/site/ack/req-002 {"request_id":"req-002","status":"skipped","detail":"value unchanged"}
```

`"ok"` means the command was forwarded, not that the PLC was written. The
control gate is checked by edge-client (see the root README,
[Control gate](../../README.md#control-gate)), so a denied control still acks
`"ok"`. A denied or failed write does not change `actuator_current`, so sending
the same value again after opening the gate is forwarded, not skipped.

The middleware does not stop anyone from publishing to edge-client's
`control/#` and `set/#` topics directly. Use broker ACLs so that only the
middleware can publish there.

## Deployment

[`deploy/middleware/`](../../deploy/middleware/) has the Scylla schema
(`init.cql`), a single-node Scylla `docker-compose.yml` (copy
`.env.docker.example` to `.env` next to it), a systemd unit
(`rust_middleware.service`) and the example env and mapping.

Build a static Linux binary with:

```bash
bash build.sh x86_64-musl iot-middleware   # bin/iot-middleware-x86_64-musl
```

## Troubleshooting

- No `connected to broker`: check `MQTT_HOST`, `MQTT_PORT` and `MQTT_PROTOCOL`.
- Exits at startup: Scylla is unreachable (`SCYLLA_NODES`) or a required env
  var is missing (`missing required env var ...`).
- Telemetry is not stored: the payload must be a plain value such as `23.5`,
  not JSON, and the topic must be in the mapping.
- A command always acks `"skipped"`: the readback already has that value. Check
  `{base}/{machine}/state/...`.
- A command acks `"error"`: the topic is not in the mapping or the value does
  not match the entry `type`. The ack `detail` and the middleware log have the
  reason.
