"""
Phase 3: Ingest Wellington CBD road network from GraphML into Neo4j.
Nodes become :Intersection, edges become :ROAD relationships.
See schema.py for the full property/index/constraint definitions.
"""

import os
from pathlib import Path

import osmnx as ox
from neo4j import GraphDatabase
from dotenv import load_dotenv

import schema

load_dotenv()

CACHE_PATH = Path(__file__).parent.parent / "data" / "wellington_cbd.graphml"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

BATCH = 500


def _build_nodes(G):
    return [
        {
            "osmid":        int(nid),
            "lat":          float(data["y"]),
            "lon":          float(data["x"]),
            "street_count": int(data.get("street_count", 0)),
        }
        for nid, data in G.nodes(data=True)
    ]


def _coerce_osmid(val):
    """osmid can be a single int or a list of ints when ways are merged."""
    if isinstance(val, list):
        val = val[0] if val else 0
    return int(val)


def _build_edges(G):
    rows = []
    for u, v, data in G.edges(data=True):
        rows.append({
            "u":           int(u),
            "v":           int(v),
            "osmid":       _coerce_osmid(data.get("osmid", 0)),
            "name":        str(data.get("name", "")),
            "highway":     str(data.get("highway", "")),
            "maxspeed":    str(data.get("maxspeed", "")),
            "length":      float(data.get("length", 0.0)),
            "speed_kph":   float(data.get("speed_kph", 0.0)),
            "travel_time": float(data.get("travel_time", 0.0)),
            "oneway":      bool(data.get("oneway", False)),
            "reversed":    bool(data.get("reversed", False)),
        })
    return rows


def ingest(driver, G):
    with driver.session() as session:
        print("Clearing existing graph data...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Applying schema...")
        schema.apply(session)

        nodes = _build_nodes(G)
        print(f"Inserting {len(nodes):,} nodes...")
        for i in range(0, len(nodes), BATCH):
            session.run(
                """
                UNWIND $batch AS n
                MERGE (i:Intersection {osmid: n.osmid})
                SET i.lat          = n.lat,
                    i.lon          = n.lon,
                    i.street_count = n.street_count
                """,
                batch=nodes[i:i + BATCH],
            )

        edges = _build_edges(G)
        print(f"Inserting {len(edges):,} edges...")
        for i in range(0, len(edges), BATCH):
            session.run(
                """
                UNWIND $batch AS e
                MATCH (a:Intersection {osmid: e.u})
                MATCH (b:Intersection {osmid: e.v})
                CREATE (a)-[r:ROAD]->(b)
                SET r.osmid        = e.osmid,
                    r.name         = e.name,
                    r.highway      = e.highway,
                    r.maxspeed     = e.maxspeed,
                    r.length       = e.length,
                    r.speed_kph    = e.speed_kph,
                    r.travel_time  = e.travel_time,
                    r.oneway       = e.oneway,
                    r.reversed     = e.reversed
                """,
                batch=edges[i:i + BATCH],
            )

        # Phase 10: create a TrafficLight for every intersection
        print("Creating :TrafficLight nodes...")
        session.run(
            """
            MATCH (i:Intersection)
            CREATE (i)-[:HAS_LIGHT]->(t:TrafficLight {
                osmid:           i.osmid,
                cycle_time:      90,
                current_phase:   'NS_GREEN',
                phase_updated_at: datetime().epochMillis,
                green_ns:        0.5,
                green_ew:        0.5
            })
            """
        )

    print("Ingest complete.")


def main():
    if not CACHE_PATH.exists():
        raise FileNotFoundError(f"GraphML not found at {CACHE_PATH}. Run download_network.py first.")

    print(f"Loading graph from {CACHE_PATH}")
    G = ox.load_graphml(CACHE_PATH)
    print(f"Nodes: {len(G.nodes):,}  Edges: {len(G.edges):,}")

    print(f"Connecting to Neo4j at {NEO4J_URI} as {NEO4J_USER}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()

    ingest(driver, G)
    driver.close()


if __name__ == "__main__":
    main()
