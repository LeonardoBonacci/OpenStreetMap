"""
Phase 8 — GDS Graph Analytics (src/gds_analytics.py)

Runs Neo4j Graph Data Science algorithms on the road network to identify
critical infrastructure:

  1. Betweenness Centrality — scores intersections by how many shortest paths
     pass through them (bottleneck detection)
  2. PageRank — scores intersections by importance from a flow/connectivity
     perspective

Results are written back to :Intersection nodes as `criticality` and `pagerank`
properties, enabling combined live-traffic + structural-importance queries.

Usage:
    python src/gds_analytics.py          # run analytics, print top-10 summary
    python src/gds_analytics.py --query  # also run example insight queries

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

GDS_GRAPH_NAME = "road-analytics"


# ---------------------------------------------------------------------------
# GDS projection
# ---------------------------------------------------------------------------

def ensure_projection(session):
    """Create or replace the GDS in-memory graph projection."""
    exists = session.run(
        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
        name=GDS_GRAPH_NAME,
    ).single()["exists"]

    if exists:
        print(f"  Dropping existing projection '{GDS_GRAPH_NAME}'…")
        session.run("CALL gds.graph.drop($name)", name=GDS_GRAPH_NAME)

    print(f"  Creating GDS projection '{GDS_GRAPH_NAME}'…")
    result = session.run(
        """
        CALL gds.graph.project(
          $name,
          'Intersection',
          {ROAD: {properties: ['travel_time', 'length']}}
        )
        YIELD nodeCount, relationshipCount
        RETURN nodeCount, relationshipCount
        """,
        name=GDS_GRAPH_NAME,
    ).single()
    print(f"  Projected {result['nodeCount']} nodes, {result['relationshipCount']} relationships")


# ---------------------------------------------------------------------------
# Betweenness Centrality
# ---------------------------------------------------------------------------

def run_betweenness(session):
    """Run betweenness centrality and write scores back to the graph."""
    print("\n▶ Running Betweenness Centrality (weighted by travel_time)…")
    result = session.run(
        """
        CALL gds.betweenness.write($name, {
          writeProperty: 'criticality',
          relationshipWeightProperty: 'travel_time'
        })
        YIELD nodePropertiesWritten, computeMillis
        RETURN nodePropertiesWritten, computeMillis
        """,
        name=GDS_GRAPH_NAME,
    ).single()
    print(f"  Written to {result['nodePropertiesWritten']} nodes "
          f"in {result['computeMillis']} ms")


# ---------------------------------------------------------------------------
# PageRank
# ---------------------------------------------------------------------------

def run_pagerank(session):
    """Run PageRank and write scores back to the graph."""
    print("\n▶ Running PageRank (weighted by travel_time)…")
    result = session.run(
        """
        CALL gds.pageRank.write($name, {
          writeProperty: 'pagerank',
          relationshipWeightProperty: 'travel_time',
          maxIterations: 20,
          dampingFactor: 0.85
        })
        YIELD nodePropertiesWritten, computeMillis
        RETURN nodePropertiesWritten, computeMillis
        """,
        name=GDS_GRAPH_NAME,
    ).single()
    print(f"  Written to {result['nodePropertiesWritten']} nodes "
          f"in {result['computeMillis']} ms")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_top_critical(session, n=10):
    """Print the top-N intersections by betweenness centrality."""
    print(f"\n{'='*60}")
    print(f" Top {n} Critical Intersections (Betweenness Centrality)")
    print(f"{'='*60}")
    records = session.run(
        """
        MATCH (i:Intersection)
        WHERE i.criticality IS NOT NULL
        OPTIONAL MATCH (i)-[r:ROAD]-()
        WITH i, collect(DISTINCT r.name) AS streets
        RETURN i.osmid AS osmid,
               round(i.criticality, 4) AS criticality,
               round(i.pagerank, 6) AS pagerank,
               i.current_speed AS live_speed,
               [s IN streets WHERE s <> '' | s][0..3] AS streets
        ORDER BY i.criticality DESC
        LIMIT $n
        """,
        n=n,
    ).data()

    print(f"{'osmid':<12} {'criticality':>12} {'pagerank':>10} {'live_speed':>11} streets")
    print(f"{'-'*12} {'-'*12} {'-'*10} {'-'*11} {'-'*20}")
    for r in records:
        speed = f"{r['live_speed']:.1f}" if r['live_speed'] is not None else "—"
        streets = ", ".join(r["streets"]) if r["streets"] else "—"
        print(f"{r['osmid']:<12} {r['criticality']:>12} {r['pagerank']:>10} {speed:>11} {streets}")


def run_insight_queries(session):
    """Run example queries that combine criticality with live traffic."""
    print(f"\n{'='*60}")
    print(" Insight: Critical bottlenecks currently congested")
    print(f"{'='*60}")
    records = session.run(
        """
        MATCH (i:Intersection)
        WHERE i.current_speed IS NOT NULL
          AND i.current_speed < 15
          AND i.criticality > 0.01
        RETURN i.osmid AS osmid,
               round(i.criticality, 4) AS criticality,
               round(i.current_speed, 1) AS speed_kph
        ORDER BY i.criticality DESC
        LIMIT 10
        """
    ).data()

    if records:
        print(f"{'osmid':<12} {'criticality':>12} {'speed_kph':>10}")
        print(f"{'-'*12} {'-'*12} {'-'*10}")
        for r in records:
            print(f"{r['osmid']:<12} {r['criticality']:>12} {r['speed_kph']:>10}")
    else:
        print("  (No critical intersections are currently congested)")

    print(f"\n{'='*60}")
    print(" Insight: Advisories affecting high-criticality intersections")
    print(f"{'='*60}")
    records = session.run(
        """
        MATCH (a:Advisory)-[:AFFECTS_ROAD]->(i:Intersection)
        WHERE i.criticality > 0.01
        RETURN a.title AS advisory,
               count(i) AS critical_intersections_affected,
               round(avg(i.criticality), 4) AS avg_criticality
        ORDER BY avg_criticality DESC
        """
    ).data()

    if records:
        for r in records:
            print(f"  • {r['advisory']}")
            print(f"    {r['critical_intersections_affected']} critical intersections, "
                  f"avg criticality {r['avg_criticality']}")
    else:
        print("  (No advisories loaded yet — run load_advisories.py first)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Connecting to Neo4j at {NEO4J_URI}…")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()

    with driver.session() as session:
        ensure_projection(session)
        run_betweenness(session)
        run_pagerank(session)
        print_top_critical(session)

        if "--query" in sys.argv:
            run_insight_queries(session)

    driver.close()
    print("\n✓ GDS analytics complete. Properties 'criticality' and 'pagerank' "
          "are now on every :Intersection node.")


if __name__ == "__main__":
    main()
