CREATE KEYSPACE IF NOT EXISTS iot_middleware
  WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

-- TELEMETRY: high-write, time-series, TTL
-- Partition key: (machine, key, day). A query such as "last hour of temp_tank
--   on area2/machine_c" hits one partition. The day bucket keeps partitions
--   from growing without bound.
-- ts  : unix epoch millis (bigint)
-- day : start of the UTC day, e.g. 2024-05-14 00:00:00 UTC = 1715644800000,
--       computed in the app as floor(ts / 86400000) * 86400000
CREATE TABLE IF NOT EXISTS iot_middleware.telemetry (
    machine     text,
    key         text,
    day         date,            -- bucket, derived from ts (start-of-day UTC millis)
    ts          bigint,          -- unix epoch millis
    value       double,          -- all monitoring values are numeric
    unit        text,
    PRIMARY KEY ((machine, key, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 7776000;   -- 90 days

-- SETPOINT HISTORY: audit-grade, append-only
CREATE TABLE IF NOT EXISTS iot_middleware.setpoint_history (
    machine     text,
    key         text,
    ts          bigint,          -- unix epoch millis
    value       double,
    unit        text,
    PRIMARY KEY ((machine, key), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- SETPOINT CURRENT: 1 row per (machine, key), latest value
CREATE TABLE IF NOT EXISTS iot_middleware.setpoint_current (
    machine     text,
    key         text,
    ts          bigint,          -- unix epoch millis
    value       double,
    unit        text,
    PRIMARY KEY ((machine, key))
);

-- ACTUATOR HISTORY: audit-grade, append-only
CREATE TABLE IF NOT EXISTS iot_middleware.actuator_history (
    machine     text,
    key         text,
    ts          bigint,          -- unix epoch millis
    state       boolean,
    PRIMARY KEY ((machine, key), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- ACTUATOR CURRENT: latest actuator state
CREATE TABLE IF NOT EXISTS iot_middleware.actuator_current (
    machine     text,
    key         text,
    ts          bigint,          -- unix epoch millis
    state       boolean,
    PRIMARY KEY ((machine, key))
);

-- HEARTBEAT HISTORY: append-only liveness log, TTL 30 days.
-- Low volume (about 1 row/10s per machine, roughly 26k rows/machine/month), so
-- no bucketing. "When did machine X disconnect in the last 30 days" hits one
-- partition.
CREATE TABLE IF NOT EXISTS iot_middleware.heartbeat_history (
    machine     text,
    ts          bigint,          -- unix epoch millis
    status      text,            -- "connected" | "disconnected"
    PRIMARY KEY ((machine), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 2592000;  -- 30 days

-- HEARTBEAT CURRENT: 1 row per machine, latest status.
-- LWW upsert: a write with a newer ts replaces the existing row.
CREATE TABLE IF NOT EXISTS iot_middleware.heartbeat_current (
    machine     text,
    ts          bigint,          -- unix epoch millis
    status      text,
    PRIMARY KEY ((machine))
);

-- PLC STATUS HISTORY: append-only log of the edge to PLC Modbus link.
-- Heartbeat covers edge to MQTT (process and network). plc_status covers edge
-- to PLC (whether the edge can read Modbus). The case that matters most is an
-- edge that is alive while its PLC is down, so it is stored separately.
-- Low volume: the edge publishes on change with retain (at most 1 row per
-- event per machine), so no bucketing. TTL 30 days, same as heartbeat.
CREATE TABLE IF NOT EXISTS iot_middleware.plc_status_history (
    machine     text,
    ts          bigint,          -- unix epoch millis
    status      text,            -- "connected" | "disconnected"
    PRIMARY KEY ((machine), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 2592000;  -- 30 days

-- PLC STATUS CURRENT: 1 row per machine, latest Modbus link status.
-- LWW upsert: a write with a newer ts replaces the existing row.
CREATE TABLE IF NOT EXISTS iot_middleware.plc_status_current (
    machine     text,
    ts          bigint,          -- unix epoch millis
    status      text,
    PRIMARY KEY ((machine))
);

-- RESOURCE HISTORY: append-only CPU/memory/IP snapshots of each edge node.
-- Same volume as heartbeat (about 1/10s per machine). TTL 30 days.
-- `memory_pct` and `cpu_pct` are not range-checked (0-100) here. Outliers are
-- left for the dashboard to flag.
CREATE TABLE IF NOT EXISTS iot_middleware.resource_history (
    machine     text,
    ts          bigint,          -- unix epoch millis
    memory_pct  double,
    cpu_pct     double,
    ip          text,
    PRIMARY KEY ((machine), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 2592000;  -- 30 days

-- RESOURCE CURRENT: latest snapshot per machine.
CREATE TABLE IF NOT EXISTS iot_middleware.resource_current (
    machine     text,
    ts          bigint,          -- unix epoch millis
    memory_pct  double,
    cpu_pct     double,
    ip          text,
    PRIMARY KEY ((machine))
);

-- ============================================================================
-- SECONDARY INDEXES: filter by `machine` without `key`
-- ============================================================================
-- setpoint_current, actuator_current, setpoint_history and actuator_history
-- are partitioned by (machine, key), so WHERE machine = ? alone needs these
-- indexes. Without them Scylla rejects the query with "Cannot execute this
-- query as it might involve data filtering".
--
-- The heartbeat, plc_status and resource tables need no index: their
-- partition key is `machine` alone.
--
-- Setpoint and actuator rows are written on change, not as dense time series,
-- so the extra write cost of a global index is acceptable.

CREATE INDEX IF NOT EXISTS setpoint_current_by_machine
  ON iot_middleware.setpoint_current (machine);

CREATE INDEX IF NOT EXISTS actuator_current_by_machine
  ON iot_middleware.actuator_current (machine);

CREATE INDEX IF NOT EXISTS setpoint_history_by_machine
  ON iot_middleware.setpoint_history (machine);

CREATE INDEX IF NOT EXISTS actuator_history_by_machine
  ON iot_middleware.actuator_history (machine);

-- TELEMETRY has no such index. It is the table with the most writes, and a
-- Scylla global secondary index is a materialized view, so every insert would
-- also write to the view. That is fine for setpoint and actuator, but could
-- slow down the busiest write path here.
--
-- If telemetry ever needs a by-machine filter, measure the current write
-- throughput first (nodetool tablestats or Scylla metrics), then enable:
--
-- CREATE INDEX IF NOT EXISTS telemetry_by_machine
--   ON iot_middleware.telemetry (machine);
