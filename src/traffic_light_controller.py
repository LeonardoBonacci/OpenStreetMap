"""
Phase 10 — Traffic Light Controller (src/traffic_light_controller.py)

A demand-responsive traffic-light controller that reads approach-sensor data
from every inbound :ROAD relationship at each intersection, computes an
optimal green-time split proportional to demand, and writes the result back
to the :TrafficLight node.

Algorithm (per intersection, each cycle):
    1. Gather sensor_queue and sensor_arrival_rate from all inbound ROADs.
    2. Classify each inbound road as 'NS' or 'EW' based on bearing.
    3. Sum demand per direction: demand = queue_length + arrival_rate * weight.
    4. Allocate green fraction: green_ns = ns_demand / total_demand (min 0.2).
    5. Write green_ns, green_ew, current_phase to the :TrafficLight node.

The controller runs in a loop (default every 10 seconds) and adjusts all
traffic lights in the network simultaneously.

Usage:
    python src/traffic_light_controller.py              # continuous loop
    python src/traffic_light_controller.py --once       # single pass then exit
    python src/traffic_light_controller.py --interval 5 # 5-second cycle

Environment (.env):
    NEO4J_URI        bolt://localhost:7687
    NEO4J_USER       neo4j
    NEO4J_PASSWORD   <your password>
"""

import argparse
import math
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

DEFAULT_INTERVAL = 10  # seconds between controller cycles
MIN_GREEN_FRAC   = 0.2  # minimum green fraction per direction
ARRIVAL_WEIGHT   = 2.0  # weight for arrival_rate in demand formula


# ---------------------------------------------------------------------------
# Bearing helper — classify an approach as NS or EW
# ---------------------------------------------------------------------------

def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute initial bearing from (lat1,lon1) to (lat2,lon2) in degrees [0,360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_lambda = math.radians(lon2 - lon1)
    x = math.sin(d_lambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lambda)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def direction_class(bearing_deg: float) -> str:
    """Classify a bearing into 'NS' or 'EW'."""
    # NS = roughly 315–45° (northbound) or 135–225° (southbound)
    # EW = everything else
    if 315 <= bearing_deg or bearing_deg < 45:
        return "NS"
    elif 135 <= bearing_deg < 225:
        return "NS"
    else:
        return "EW"


# ---------------------------------------------------------------------------
# Core controller logic
# ---------------------------------------------------------------------------

_GATHER_CYPHER = """
MATCH (src:Intersection)-[r:ROAD]->(dst:Intersection)-[:HAS_LIGHT]->(t:TrafficLight)
WHERE r.sensor_queue IS NOT NULL
RETURN dst.osmid        AS osmid,
       src.lat          AS src_lat,
       src.lon          AS src_lon,
       dst.lat          AS dst_lat,
       dst.lon          AS dst_lon,
       r.sensor_queue        AS queue,
       r.sensor_arrival_rate AS rate
"""

_UPDATE_LIGHT_CYPHER = """
MATCH (i:Intersection {osmid: $osmid})-[:HAS_LIGHT]->(t:TrafficLight)
SET t.green_ns        = $green_ns,
    t.green_ew        = $green_ew,
    t.current_phase   = $phase,
    t.phase_updated_at = $ts
"""


def control_cycle(session) -> int:
    """Run one control cycle. Returns the number of lights updated."""
    result = session.run(_GATHER_CYPHER)
    records = list(result)

    if not records:
        return 0

    # Group by destination intersection
    demand_by_intersection: dict[int, dict[str, float]] = {}
    for rec in records:
        osmid = rec["osmid"]
        if osmid not in demand_by_intersection:
            demand_by_intersection[osmid] = {"NS": 0.0, "EW": 0.0}

        b = bearing(rec["src_lat"], rec["src_lon"], rec["dst_lat"], rec["dst_lon"])
        direction = direction_class(b)
        demand = (rec["queue"] or 0) + ARRIVAL_WEIGHT * (rec["rate"] or 0.0)
        demand_by_intersection[osmid][direction] += demand

    # Compute green splits and write back
    ts = datetime.now(timezone.utc).isoformat()
    updated = 0
    for osmid, demands in demand_by_intersection.items():
        total = demands["NS"] + demands["EW"]
        if total == 0:
            green_ns = 0.5
        else:
            green_ns = demands["NS"] / total

        # Enforce minimum green
        green_ns = max(MIN_GREEN_FRAC, min(1 - MIN_GREEN_FRAC, green_ns))
        green_ew = 1.0 - green_ns

        # Phase label based on majority demand
        phase = "NS_GREEN" if green_ns >= green_ew else "EW_GREEN"

        session.run(
            _UPDATE_LIGHT_CYPHER,
            osmid=osmid,
            green_ns=round(green_ns, 3),
            green_ew=round(green_ew, 3),
            phase=phase,
            ts=ts,
        ).consume()
        updated += 1

    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(interval: int, once: bool):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print(f"Traffic Light Controller connected to {NEO4J_URI}")
    print(f"Cycle interval: {interval}s  |  Min green fraction: {MIN_GREEN_FRAC}")
    print("─" * 60)

    try:
        with driver.session() as session:
            while True:
                updated = control_cycle(session)
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"  [{now}] Updated {updated} traffic light(s)")
                if once:
                    break
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demand-responsive traffic light controller")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"Seconds between control cycles (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--once", action="store_true",
                        help="Run a single control cycle and exit")
    args = parser.parse_args()
    run(interval=args.interval, once=args.once)
