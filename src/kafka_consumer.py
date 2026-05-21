"""
Phase 7 — Kafka Consumer (src/kafka_consumer.py)

Consumes sensor readings from the 'sensor-readings' topic and writes live
traffic state back into Neo4j :Intersection nodes. Emits CONGESTION alerts
to the 'traffic-alerts' topic when avg_speed_kph < 15.

Neo4j writes (per message):
    MATCH (i:Intersection {osmid: $osmid})
    SET i.current_flow  = $vehicle_count,
        i.current_speed = $avg_speed_kph,
        i.last_seen     = $ts

Alert payload (JSON) → topic 'traffic-alerts':
    {
      "type":       "CONGESTION",
      "osmid":      <int>,
      "street":     <string | null>,   # name of a road at that intersection
      "speed_kph":  <float>,
      "ts":         <ISO-8601 UTC string>
    }

Usage:
    python src/kafka_consumer.py    # runs until Ctrl-C

Environment (.env):
    NEO4J_URI        bolt://localhost:7687
    NEO4J_USER       neo4j
    NEO4J_PASSWORD   <your password>
    KAFKA_BOOTSTRAP  localhost:9092          (optional)
    CONSUMER_GROUP   traffic-consumer        (optional)
"""

import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI       = os.getenv("NEO4J_URI",       "bolt://localhost:7687")
NEO4J_USER      = os.getenv("NEO4J_USER",      "neo4j")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD",  "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP  = os.getenv("CONSUMER_GROUP",  "traffic-consumer")

TOPIC_IN  = "sensor-readings"
TOPIC_OUT = "traffic-alerts"
CONGESTION_THRESHOLD = 15.0   # km/h


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

_UPDATE_CYPHER = """
MATCH (i:Intersection {osmid: $osmid})
SET i.current_flow  = $vehicle_count,
    i.current_speed = $avg_speed_kph,
    i.last_seen     = $ts
"""

_STREET_CYPHER = """
MATCH (i:Intersection {osmid: $osmid})-[r:ROAD]-()
WHERE r.name IS NOT NULL AND r.name <> ''
RETURN r.name AS name LIMIT 1
"""


def write_to_neo4j(session, msg: dict):
    session.run(
        _UPDATE_CYPHER,
        osmid=msg["intersection_osmid"],
        vehicle_count=msg["vehicle_count"],
        avg_speed_kph=msg["avg_speed_kph"],
        ts=msg["ts"],
    )


def lookup_street(session, osmid: int) -> str | None:
    result = session.run(_STREET_CYPHER, osmid=osmid)
    record = result.single()
    return record["name"] if record else None


# ---------------------------------------------------------------------------
# Alert helper
# ---------------------------------------------------------------------------

def build_alert(osmid: int, street: str | None, speed_kph: float) -> dict:
    return {
        "type":      "CONGESTION",
        "osmid":     osmid,
        "street":    street,
        "speed_kph": speed_kph,
        "ts":        datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        consumer = KafkaConsumer(
            TOPIC_IN,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        alert_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    except NoBrokersAvailable:
        print(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}. Is docker compose up?", file=sys.stderr)
        sys.exit(1)

    print(f"Consuming from '{TOPIC_IN}'  |  alerts → '{TOPIC_OUT}'  (Ctrl-C to stop)")

    try:
        with driver.session() as session:
            for kafka_msg in consumer:
                msg = kafka_msg.value
                osmid     = msg["intersection_osmid"]
                speed_kph = msg["avg_speed_kph"]

                write_to_neo4j(session, msg)

                if speed_kph < CONGESTION_THRESHOLD:
                    street = lookup_street(session, osmid)
                    alert  = build_alert(osmid, street, speed_kph)
                    alert_producer.send(TOPIC_OUT, alert)
                    street_label = f" on {street}" if street else ""
                    print(
                        f"  [ALERT] CONGESTION{street_label}  "
                        f"osmid={osmid}  speed={speed_kph} km/h"
                    )
                else:
                    print(
                        f"  [OK]    osmid={osmid:>12}  "
                        f"speed={speed_kph:>5.1f} km/h  "
                        f"vehicles={msg['vehicle_count']:>3}"
                    )

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        alert_producer.flush()
        alert_producer.close()
        consumer.close()
        driver.close()


if __name__ == "__main__":
    run()
