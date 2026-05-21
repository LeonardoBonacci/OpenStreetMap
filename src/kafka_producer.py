"""
Phase 7 — Kafka Producer (src/kafka_producer.py)

Simulates IoT traffic sensors at Wellington CBD intersections.
Reads intersection osmids from Neo4j, then emits a continuous stream of
sensor readings to the 'sensor-readings' Kafka topic.

Message payload (JSON):
    {
      "intersection_osmid": <int>,
      "vehicle_count":      <int>,    # 0–60 vehicles/min
      "avg_speed_kph":      <float>,  # 0–70 km/h
      "ts":                 <ISO-8601 UTC string>
    }

Roughly 5 % of messages simulate a congestion event (avg_speed < 15 km/h).

Usage:
    python src/kafka_producer.py              # runs until Ctrl-C
    python src/kafka_producer.py --count 50  # emit exactly 50 messages and exit

Environment (.env):
    NEO4J_URI        bolt://localhost:7687
    NEO4J_USER       neo4j
    NEO4J_PASSWORD   <your password>
    KAFKA_BOOTSTRAP  localhost:9092          (optional)
    PRODUCER_DELAY   0.5                     (seconds between messages, optional)
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI       = os.getenv("NEO4J_URI",       "bolt://localhost:7687")
NEO4J_USER      = os.getenv("NEO4J_USER",      "neo4j")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD",  "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PRODUCER_DELAY  = float(os.getenv("PRODUCER_DELAY", "0.5"))

TOPIC = "sensor-readings"
TOPIC_APPROACH = "approach-sensors"


def fetch_osmids(driver) -> list[int]:
    with driver.session() as session:
        result = session.run("MATCH (i:Intersection) RETURN i.osmid AS osmid")
        return [r["osmid"] for r in result]


def fetch_road_pairs(driver) -> list[tuple[int, int]]:
    """Fetch all (from_osmid, to_osmid) pairs for ROAD relationships."""
    with driver.session() as session:
        result = session.run(
            "MATCH (a:Intersection)-[:ROAD]->(b:Intersection) "
            "RETURN a.osmid AS u, b.osmid AS v"
        )
        return [(r["u"], r["v"]) for r in result]


def make_reading(osmid: int) -> dict:
    congestion = random.random() < 0.05           # ~5 % congestion spike
    if congestion:
        avg_speed    = round(random.uniform(2.0, 14.9), 1)
        vehicle_count = random.randint(30, 60)
    else:
        avg_speed    = round(random.uniform(20.0, 70.0), 1)
        vehicle_count = random.randint(0, 30)

    return {
        "intersection_osmid": osmid,
        "vehicle_count":      vehicle_count,
        "avg_speed_kph":      avg_speed,
        "ts":                 datetime.now(timezone.utc).isoformat(),
    }


def make_approach_reading(from_osmid: int, to_osmid: int) -> dict:
    """Simulate an inductive-loop sensor on the approach to an intersection."""
    queue_length  = random.randint(0, 40)
    arrival_rate  = round(random.uniform(0.0, 30.0), 1)
    occupancy_pct = round(random.uniform(0.0, 100.0), 1)
    return {
        "from_osmid":    from_osmid,
        "to_osmid":      to_osmid,
        "queue_length":  queue_length,
        "arrival_rate":  arrival_rate,
        "occupancy_pct": occupancy_pct,
        "ts":            datetime.now(timezone.utc).isoformat(),
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def run(count: int | None = None):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    osmids = fetch_osmids(driver)
    road_pairs = fetch_road_pairs(driver)
    driver.close()

    if not osmids:
        print("No intersections found in Neo4j — run ingest_to_neo4j.py first.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(osmids)} intersections, {len(road_pairs)} road segments from Neo4j.")

    try:
        producer = build_producer()
    except NoBrokersAvailable:
        print(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}. Is docker compose up?", file=sys.stderr)
        sys.exit(1)

    print(f"Producing to '{TOPIC}' + '{TOPIC_APPROACH}' at {KAFKA_BOOTSTRAP}  (Ctrl-C to stop)")
    sent = 0
    try:
        while count is None or sent < count:
            # --- intersection-level reading (existing Phase 7) ---
            osmid   = random.choice(osmids)
            reading = make_reading(osmid)
            producer.send(TOPIC, reading)
            flag = " [CONGESTION]" if reading["avg_speed_kph"] < 15 else ""
            print(
                f"  osmid={reading['intersection_osmid']:>12}  "
                f"speed={reading['avg_speed_kph']:>5.1f} km/h  "
                f"vehicles={reading['vehicle_count']:>3}{flag}"
            )

            # --- approach-sensor reading (Phase 10) ---
            if road_pairs:
                pair = random.choice(road_pairs)
                approach = make_approach_reading(*pair)
                producer.send(TOPIC_APPROACH, approach)
                print(
                    f"    sensor {pair[0]}→{pair[1]}  "
                    f"queue={approach['queue_length']:>2}  "
                    f"rate={approach['arrival_rate']:>5.1f}/min  "
                    f"occ={approach['occupancy_pct']:>5.1f}%"
                )

            sent += 1
            time.sleep(PRODUCER_DELAY)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()

    print(f"Sent {sent} message(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=None,
                        help="Number of messages to send then exit (default: run forever)")
    args = parser.parse_args()
    run(args.count)
