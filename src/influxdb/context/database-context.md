# Wellington CBD Traffic Database — InfluxDB 3 Context

This document describes the InfluxDB 3 Core database that stores real-time and
historical traffic sensor data for the Wellington CBD road network.

## Connection

- **URL**: `http://localhost:8181`
- **Database**: `traffic`
- **Auth**: none (running with `--without-auth`)
- **Query language**: SQL (InfluxDB 3 uses Apache Arrow / Flight SQL)

---

## Data Source

The data originates from a simulated IoT sensor pipeline:

1. `kafka_producer.py` — publishes JSON messages every ~0.5 s to two Kafka topics
2. `src/influxdb/influx_consumer.py` — consumes both topics and writes to InfluxDB

The road network covers **273 intersections** and **539 directed road segments**
in Wellington CBD, New Zealand (bounding box: north=-41.276, south=-41.295,
east=174.785, west=174.770), based on real OpenStreetMap data.

---

## Measurements

### `traffic_sensor`

One row per intersection per sensor reading (~2 messages/s across all intersections).

| Column              | Type      | Description                                        |
|---------------------|-----------|----------------------------------------------------|
| `time`              | timestamp | UTC timestamp of the sensor reading (nanoseconds)  |
| `intersection_osmid`| tag (str) | OSM node ID of the intersection (273 unique values)|
| `vehicle_count`     | integer   | Vehicles passing per minute (0–60)                 |
| `avg_speed_kph`     | float     | Average vehicle speed in km/h (0–70)               |

**Congestion indicator**: `avg_speed_kph < 15` signals a heavy-congestion event
(simulated at ~5 % probability per reading).

Example query — slowest intersections in the last 15 minutes:
```sql
SELECT
    intersection_osmid,
    AVG(avg_speed_kph)   AS avg_speed,
    SUM(vehicle_count)   AS total_vehicles
FROM traffic_sensor
WHERE time > now() - INTERVAL '15 minutes'
GROUP BY intersection_osmid
ORDER BY avg_speed ASC
LIMIT 10
```

---

### `approach_sensor`

One row per directed road segment per reading (~2 messages/s across all segments).
Models inductive-loop detectors embedded in road surfaces on the approach to each
intersection.

| Column          | Type      | Description                                              |
|-----------------|-----------|----------------------------------------------------------|
| `time`          | timestamp | UTC timestamp of the sensor reading (nanoseconds)        |
| `from_osmid`    | tag (str) | OSM node ID of the upstream (origin) intersection        |
| `to_osmid`      | tag (str) | OSM node ID of the downstream (destination) intersection |
| `queue_length`  | integer   | Vehicles queued on this approach (0–40)                  |
| `arrival_rate`  | float     | Vehicles arriving per minute (0.0–30.0)                  |
| `occupancy_pct` | float     | Loop detector occupancy percentage (0.0–100.0)           |

**Congestion indicator**: `queue_length > 20` or `occupancy_pct > 70` indicates
a backed-up approach.

Example query — most congested approaches right now:
```sql
SELECT
    from_osmid,
    to_osmid,
    AVG(queue_length)   AS avg_queue,
    AVG(occupancy_pct)  AS avg_occupancy
FROM approach_sensor
WHERE time > now() - INTERVAL '2 minutes'
GROUP BY from_osmid, to_osmid
ORDER BY avg_queue DESC
LIMIT 10
```

---

## Common Query Patterns

### Network-wide congestion summary (last 5 min)
```sql
SELECT
    COUNT(DISTINCT intersection_osmid)              AS total_intersections,
    COUNT(CASE WHEN avg_speed_kph < 15 THEN 1 END) AS congested_readings,
    AVG(avg_speed_kph)                              AS network_avg_speed,
    AVG(vehicle_count)                              AS network_avg_flow
FROM traffic_sensor
WHERE time > now() - INTERVAL '5 minutes'
```

### Rush-hour speed patterns (07:00–09:00)
```sql
SELECT
    intersection_osmid,
    AVG(avg_speed_kph) AS morning_avg_speed,
    COUNT(*)           AS reading_count
FROM traffic_sensor
WHERE extract(hour FROM time) >= 7
  AND extract(hour FROM time) <  9
GROUP BY intersection_osmid
ORDER BY morning_avg_speed ASC
LIMIT 10
```

### Compare two specific intersections over time
```sql
SELECT
    date_trunc('minute', time) AS minute_bucket,
    intersection_osmid,
    AVG(avg_speed_kph)         AS avg_speed
FROM traffic_sensor
WHERE intersection_osmid IN ('2584656974', '2584657001')
  AND time > now() - INTERVAL '1 hour'
GROUP BY minute_bucket, intersection_osmid
ORDER BY minute_bucket, intersection_osmid
```

### Sustained congestion — segments stuck for > 10 min
```sql
SELECT
    from_osmid,
    to_osmid,
    MIN(time)              AS congestion_start,
    MAX(time)              AS last_seen,
    AVG(queue_length)      AS avg_queue,
    COUNT(*)               AS readings
FROM approach_sensor
WHERE queue_length > 20
  AND time > now() - INTERVAL '30 minutes'
GROUP BY from_osmid, to_osmid
HAVING COUNT(*) > 20
ORDER BY avg_queue DESC
```

### Busiest intersection right now
```sql
SELECT
    intersection_osmid,
    AVG(vehicle_count)  AS avg_flow,
    AVG(avg_speed_kph)  AS avg_speed
FROM traffic_sensor
WHERE time > now() - INTERVAL '3 minutes'
GROUP BY intersection_osmid
ORDER BY avg_flow DESC
LIMIT 1
```

### Throughput vs. speed tradeoff (volume/speed ratio)
```sql
SELECT
    intersection_osmid,
    AVG(vehicle_count / NULLIF(avg_speed_kph, 0)) AS density_proxy,
    AVG(vehicle_count)                             AS avg_flow,
    AVG(avg_speed_kph)                             AS avg_speed
FROM traffic_sensor
WHERE time > now() - INTERVAL '10 minutes'
GROUP BY intersection_osmid
ORDER BY density_proxy DESC
LIMIT 10
```

---

## Notes for LLM Agents

- **Tag values are strings** — always quote them in WHERE clauses:
  `WHERE intersection_osmid = '268939537'`
- **Time arithmetic**: use `now()` and `INTERVAL '…'` syntax.
- **No joins across measurements** are needed for most queries; each measurement
  is self-contained.
- The database contains **live data** — very recent windows (< 1 min) may have
  only a handful of rows; prefer ≥ 5-minute windows for meaningful aggregates.
- All timestamps are stored in UTC.
