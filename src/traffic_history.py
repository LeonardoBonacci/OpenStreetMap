"""
Phase 9 — Temporal Traffic History (src/traffic_history.py)

Queries the :TrafficSnapshot nodes to surface time-based traffic patterns:

  1. Rush-hour hotspots — slowest intersections during morning (7–9am) and
     evening (4:30–6:30pm) peaks
  2. Chronic congestion — intersections with the most congestion events overall
  3. Current congestion duration — how long an intersection has been slow

The consumer (kafka_consumer.py) creates :TrafficSnapshot nodes as it processes
the stream. This script provides analytics over the accumulated history.

Usage:
    python src/traffic_history.py              # run all summary queries
    python src/traffic_history.py --morning    # morning rush-hour only
    python src/traffic_history.py --evening    # evening rush-hour only
    python src/traffic_history.py --chronic    # most-congested intersections
    python src/traffic_history.py --duration   # active congestion durations

Environment (.env):
    NEO4J_URI           bolt://localhost:7687
    NEO4J_USER          neo4j
    NEO4J_PASSWORD      <your password>
"""

import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

CONGESTION_THRESHOLD = 15.0  # km/h


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

_MORNING_RUSH = """
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE time(s.recorded_at) >= time("07:00")
  AND time(s.recorded_at) <  time("09:00")
WITH i, avg(s.speed) AS avg_speed, count(s) AS readings
WHERE readings >= 3
RETURN i.osmid AS osmid, round(avg_speed, 1) AS avg_speed_kph, readings
ORDER BY avg_speed ASC
LIMIT 10
"""

_EVENING_RUSH = """
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE time(s.recorded_at) >= time("16:30")
  AND time(s.recorded_at) <  time("18:30")
WITH i, avg(s.speed) AS avg_speed, count(s) AS readings
WHERE readings >= 3
RETURN i.osmid AS osmid, round(avg_speed, 1) AS avg_speed_kph, readings
ORDER BY avg_speed ASC
LIMIT 10
"""

_CHRONIC_CONGESTION = """
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE s.speed < $threshold
WITH i, count(s) AS congestion_events, avg(s.speed) AS avg_congested_speed
RETURN i.osmid AS osmid,
       congestion_events,
       round(avg_congested_speed, 1) AS avg_congested_kph
ORDER BY congestion_events DESC
LIMIT 10
"""

_CONGESTION_DURATION = """
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE s.speed < $threshold
WITH i, max(s.recorded_at) AS latest, min(s.recorded_at) AS earliest,
     count(s) AS events
WHERE events >= 2
RETURN i.osmid AS osmid,
       toString(earliest) AS congested_since,
       toString(latest) AS last_congested,
       duration.between(earliest, latest).minutes AS span_minutes,
       events
ORDER BY span_minutes DESC
LIMIT 10
"""

_SNAPSHOT_COUNT = """
MATCH (s:TrafficSnapshot)
RETURN count(s) AS total_snapshots
"""


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def print_table(title, records, columns):
    """Pretty-print query results as a simple table."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")
    if not records:
        print("  (no data — let the consumer run for a while first)")
        return
    # Header
    header = "  ".join(f"{col:>18}" for col in columns)
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for rec in records:
        row = "  ".join(f"{str(rec[col]):>18}" for col in columns)
        print(f"  {row}")


def run_query(session, title, cypher, columns, **params):
    result = session.run(cypher, **params)
    records = [dict(r) for r in result]
    print_table(title, records, columns)
    return records


def main():
    args = set(sys.argv[1:])
    run_all = not args or "--all" in args

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            # Always show snapshot count
            result = session.run(_SNAPSHOT_COUNT)
            count = result.single()["total_snapshots"]
            print(f"\nTotal :TrafficSnapshot nodes in graph: {count}")

            if run_all or "--morning" in args:
                run_query(session,
                          "Morning Rush-Hour Slowest Intersections (07:00–09:00)",
                          _MORNING_RUSH,
                          ["osmid", "avg_speed_kph", "readings"])

            if run_all or "--evening" in args:
                run_query(session,
                          "Evening Rush-Hour Slowest Intersections (16:30–18:30)",
                          _EVENING_RUSH,
                          ["osmid", "avg_speed_kph", "readings"])

            if run_all or "--chronic" in args:
                run_query(session,
                          "Chronically Congested Intersections (most events < 15 km/h)",
                          _CHRONIC_CONGESTION,
                          ["osmid", "congestion_events", "avg_congested_kph"],
                          threshold=CONGESTION_THRESHOLD)

            if run_all or "--duration" in args:
                run_query(session,
                          "Active Congestion Duration (longest spans)",
                          _CONGESTION_DURATION,
                          ["osmid", "span_minutes", "events", "congested_since"],
                          threshold=CONGESTION_THRESHOLD)

    finally:
        driver.close()

    print(f"\n{'=' * 60}")
    print("  Tip: combine with GDS criticality for high-impact insights:")
    print("    MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)")
    print("    WHERE s.speed < 15 AND i.criticality > 0.01")
    print("    RETURN i.osmid, i.criticality, avg(s.speed)")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
