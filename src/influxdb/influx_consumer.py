"""
Phase 11 — Kafka → InfluxDB Consumer (src/influxdb/influx_consumer.py)

Reads both Kafka topics produced by kafka_producer.py and writes every message
as structured time-series data to InfluxDB 3 Core:

  Topic              Measurement        Tags                    Fields
  ─────────────────  ─────────────────  ──────────────────────  ──────────────────────────────────
  sensor-readings    traffic_sensor     intersection_osmid      vehicle_count, avg_speed_kph
  approach-sensors   approach_sensor    from_osmid, to_osmid    queue_length, arrival_rate, occupancy_pct

Points are accumulated in memory and flushed to InfluxDB in batches of up to
BATCH_SIZE points, or at most every FLUSH_INTERVAL seconds — whichever occurs
first.  A background thread guarantees the time-based flush even when the
Kafka stream is slow.

Usage:
    python src/influxdb/influx_consumer.py          # runs until Ctrl-C
    python src/influxdb/influx_consumer.py --once   # consume until both topics
                                                     # are idle for 5 s, then exit

Environment (.env):
    KAFKA_BOOTSTRAP         localhost:9092   (optional)
    INFLUX_DB_INSTANCE_URL  http://localhost:8181
    INFLUX_DB_DATABASE      traffic          (optional, default: traffic)
"""

import argparse
import json
import os
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INFLUX_URL      = os.getenv("INFLUX_DB_INSTANCE_URL", "http://localhost:8181")
INFLUX_DATABASE = os.getenv("INFLUX_DB_DATABASE", "traffic")

TOPIC_SENSOR   = "sensor-readings"
TOPIC_APPROACH = "approach-sensors"

BATCH_SIZE     = 100
FLUSH_INTERVAL = 1.0   # seconds between forced flushes


# ---------------------------------------------------------------------------
# Batch writer with background flush thread
# ---------------------------------------------------------------------------

class BatchWriter:
    """Thread-safe buffer that writes to InfluxDB in batches."""

    def __init__(self, client: InfluxDBClient3):
        self._client = client
        self._buf: list[Point] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self._total = 0

    def add(self, point: Point) -> None:
        with self._lock:
            self._buf.append(point)
            ready = (
                len(self._buf) >= BATCH_SIZE
                or (time.monotonic() - self._last_flush) >= FLUSH_INTERVAL
            )
        if ready:
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            if not self._buf:
                return
            batch, self._buf = self._buf, []
            self._last_flush = time.monotonic()
        self._write(batch)

    def _write(self, batch: list[Point]) -> None:
        try:
            self._client.write(record=batch, database=INFLUX_DATABASE)
            self._total += len(batch)
            print(
                f"[influx] wrote {len(batch):3d} points  "
                f"(total: {self._total})",
                flush=True,
            )
        except Exception as exc:
            print(f"[influx] write error: {exc}", flush=True)

    def flush(self) -> None:
        """Unconditionally flush whatever is buffered."""
        self._flush()

    def run_background_flusher(self) -> None:
        """Block forever, flushing every FLUSH_INTERVAL seconds."""
        while True:
            time.sleep(FLUSH_INTERVAL)
            self._flush()


# ---------------------------------------------------------------------------
# Message → Point converters
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO-8601 UTC string; fall back to now() if malformed."""
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.now(timezone.utc)


def sensor_to_point(msg: dict) -> Point:
    """Convert a sensor-readings message to an InfluxDB Point."""
    return (
        Point("traffic_sensor")
        .tag("intersection_osmid", str(msg["intersection_osmid"]))
        .field("vehicle_count",  int(msg["vehicle_count"]))
        .field("avg_speed_kph",  float(msg["avg_speed_kph"]))
        .time(_parse_ts(msg["ts"]))
    )


def approach_to_point(msg: dict) -> Point:
    """Convert an approach-sensors message to an InfluxDB Point."""
    return (
        Point("approach_sensor")
        .tag("from_osmid", str(msg["from_osmid"]))
        .tag("to_osmid",   str(msg["to_osmid"]))
        .field("queue_length",  int(msg["queue_length"]))
        .field("arrival_rate",  float(msg["arrival_rate"]))
        .field("occupancy_pct", float(msg["occupancy_pct"]))
        .time(_parse_ts(msg["ts"]))
    )


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def build_consumer(group_id: str = "influx-consumer") -> KafkaConsumer:
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                TOPIC_SENSOR,
                TOPIC_APPROACH,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=5_000,   # raises StopIteration after 5 s idle
            )
            print(f"[kafka]  connected  bootstrap={KAFKA_BOOTSTRAP}", flush=True)
            return consumer
        except NoBrokersAvailable:
            print(
                f"[kafka]  no brokers (attempt {attempt}/10) — retrying in 3 s …",
                flush=True,
            )
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def run(once: bool = False) -> None:
    influx = InfluxDBClient3(host=INFLUX_URL, database=INFLUX_DATABASE)
    writer = BatchWriter(influx)

    # Start the background flush thread (daemon so it exits with the process)
    flusher = threading.Thread(
        target=writer.run_background_flusher, daemon=True, name="influx-flusher"
    )
    flusher.start()

    consumer = build_consumer()
    print(
        f"[influx] writing to {INFLUX_URL}  database={INFLUX_DATABASE}",
        flush=True,
    )
    print(
        f"[influx] batch_size={BATCH_SIZE}  flush_interval={FLUSH_INTERVAL}s",
        flush=True,
    )

    try:
        idle_rounds = 0
        while True:
            got_messages = False
            try:
                for record in consumer:
                    got_messages = True
                    idle_rounds = 0
                    topic   = record.topic
                    payload = record.value
                    try:
                        if topic == TOPIC_SENSOR:
                            writer.add(sensor_to_point(payload))
                        elif topic == TOPIC_APPROACH:
                            writer.add(approach_to_point(payload))
                    except (KeyError, TypeError, ValueError) as exc:
                        print(f"[warn]  skipping malformed message: {exc}", flush=True)
            except StopIteration:
                # consumer_timeout_ms elapsed with no new messages
                pass

            if not got_messages:
                idle_rounds += 1
                if once and idle_rounds >= 1:
                    print("[influx] idle — exiting (--once mode)", flush=True)
                    break
                print(f"[influx] idle ({idle_rounds}) …", flush=True)

    except KeyboardInterrupt:
        print("\n[influx] interrupted", flush=True)
    finally:
        writer.flush()
        consumer.close()
        influx.close()
        print("[influx] shut down cleanly", flush=True)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → InfluxDB consumer")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Exit after topics have been idle for one poll cycle",
    )
    args = parser.parse_args()
    run(once=args.once)
