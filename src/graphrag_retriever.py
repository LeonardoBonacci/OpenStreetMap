"""
Phase 6 — GraphRAG Retriever (src/graphrag_retriever.py)

Route-aware Q&A: blends GDS Dijkstra path-finding with graph-proximity
advisory retrieval.

Algorithm
---------
1. Resolve start/end to :Intersection osmids (by street-name intersection query)
2. Project the road network into GDS (once; reuses existing projection)
3. Run gds.shortestPath.dijkstra, weighted by travel_time
4. Fetch every :Advisory linked via :AFFECTS_ROAD to an intersection on that path
5. Feed route summary + advisory texts to the LLM for a route-aware answer

The key insight: advisories are retrieved by graph proximity (which intersections
are on the route?), not keyword similarity. A pure vector RAG has no concept of
spatial adjacency.

Usage:
    python src/graphrag_retriever.py           # 3 demo queries
    python src/graphrag_retriever.py --repl    # interactive prompt

    REPL input format:
      Start: Lambton Quay / Willis Street
      End  : Courtenay Place / Tory Street
      Q    : Any disruptions on this route?

Environment (.env):
    NEO4J_URI           bolt://localhost:7687
    NEO4J_USER          neo4j
    NEO4J_PASSWORD      <your password>
    OLLAMA_MODEL        llama3.1:latest          (optional)
    OLLAMA_BASE_URL     http://localhost:11434    (optional)
"""

import os
import sys

from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI       = os.getenv("NEO4J_URI",       "bolt://localhost:7687")
NEO4J_USER      = os.getenv("NEO4J_USER",      "neo4j")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD",  "")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "llama3.1:latest")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

GDS_GRAPH_NAME = "road-graph"


# ---------------------------------------------------------------------------
# GDS graph projection
# ---------------------------------------------------------------------------

def _ensure_gds_projection(session):
    """Create the GDS in-memory projection if it doesn't already exist."""
    exists = session.run(
        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
        name=GDS_GRAPH_NAME,
    ).single()["exists"]
    if not exists:
        print(f"  Projecting GDS graph '{GDS_GRAPH_NAME}'…", flush=True)
        session.run(
            """
            CALL gds.graph.project(
              $name,
              'Intersection',
              {ROAD: {properties: ['travel_time']}}
            )
            """,
            name=GDS_GRAPH_NAME,
        )


# ---------------------------------------------------------------------------
# Intersection lookup helpers
# ---------------------------------------------------------------------------

def find_intersection(session, street_a: str, street_b: str) -> int | None:
    """Return the osmid of the intersection of two named streets, or None."""
    result = session.run(
        """
        MATCH (i:Intersection)
        WHERE EXISTS { MATCH (i)-[:ROAD {name: $street_a}]-() }
          AND EXISTS { MATCH (i)-[:ROAD {name: $street_b}]-() }
        RETURN i.osmid AS osmid
        LIMIT 1
        """,
        street_a=street_a,
        street_b=street_b,
    )
    record = result.single()
    return record["osmid"] if record else None


def find_intersection_on_street(session, street: str) -> int | None:
    """Return the osmid of the busiest intersection on a named street."""
    result = session.run(
        """
        MATCH (i:Intersection)-[:ROAD {name: $street}]-()
        RETURN i.osmid AS osmid, i.street_count AS sc
        ORDER BY sc DESC
        LIMIT 1
        """,
        street=street,
    )
    record = result.single()
    return record["osmid"] if record else None


def resolve_location(session, spec: str) -> int | None:
    """
    Parse 'Street A / Street B' → intersection osmid, or
    'Street A'                  → busiest intersection on that street.
    """
    parts = [s.strip() for s in spec.split("/")]
    if len(parts) == 2:
        osmid = find_intersection(session, parts[0], parts[1])
        if osmid:
            return osmid
        # Fallback: try each street individually
        return find_intersection_on_street(session, parts[0])
    return find_intersection_on_street(session, parts[0])


# ---------------------------------------------------------------------------
# GDS Dijkstra
# ---------------------------------------------------------------------------

def dijkstra_route(session, source_osmid: int, target_osmid: int) -> tuple[list, float]:
    """
    Return (route_osmids, total_travel_time_seconds) for the shortest
    weighted path between two intersections.
    route_osmids is an ordered list of osmids from source to target.
    """
    _ensure_gds_projection(session)
    result = session.run(
        """
        MATCH (source:Intersection {osmid: $source_osmid}),
              (target:Intersection {osmid: $target_osmid})
        CALL gds.shortestPath.dijkstra.stream($graph_name, {
          sourceNode: source,
          targetNode: target,
          relationshipWeightProperty: 'travel_time'
        })
        YIELD path, totalCost
        RETURN [n IN nodes(path) | n.osmid] AS route_osmids,
               totalCost
        """,
        source_osmid=source_osmid,
        target_osmid=target_osmid,
        graph_name=GDS_GRAPH_NAME,
    )
    record = result.single()
    if not record:
        return [], 0.0
    return record["route_osmids"], record["totalCost"]


# ---------------------------------------------------------------------------
# Advisory retrieval — graph proximity, not keyword matching
# ---------------------------------------------------------------------------

def get_route_advisories(session, route_osmids: list) -> list[dict]:
    """
    Return all :Advisory nodes linked via :AFFECTS_ROAD to any intersection
    on the route. Ordered by published date descending.
    """
    if not route_osmids:
        return []
    result = session.run(
        """
        MATCH (adv:Advisory)-[:AFFECTS_ROAD]->(i:Intersection)
        WHERE i.osmid IN $osmids
        WITH DISTINCT adv
        RETURN adv.id        AS id,
               adv.title     AS title,
               adv.text      AS text,
               adv.published AS published
        ORDER BY adv.published DESC
        """,
        osmids=route_osmids,
    )
    return [dict(r) for r in result]


# ---------------------------------------------------------------------------
# LLM synthesis
# ---------------------------------------------------------------------------

def synthesise(llm: ChatOllama, question: str, route_info: str, advisories: list[dict]) -> str:
    if advisories:
        adv_block = "\n\n".join(
            f"[{a['id']}] {a['title']} (published {a['published']}):\n{a['text']}"
            for a in advisories
        )
    else:
        adv_block = "No active advisories found for intersections along this route."

    prompt = (
        "You are a Wellington CBD traffic assistant.\n\n"
        f"Route summary: {route_info}\n\n"
        "Traffic advisories for intersections on this route (retrieved by graph "
        "proximity — these advisories are linked to intersections the route passes "
        "through):\n\n"
        f"{adv_block}\n\n"
        f"Question: {question}\n\n"
        "Answer based only on the route and advisories above. Be concise and practical."
    )
    return llm.invoke(prompt).content


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def retrieve_and_answer(
    driver,
    llm: ChatOllama,
    question: str,
    source_osmid: int,
    target_osmid: int,
) -> str:
    with driver.session() as session:
        route_osmids, total_cost = dijkstra_route(session, source_osmid, target_osmid)

    if not route_osmids:
        return "Could not find a route between those intersections in the graph."

    hops = len(route_osmids) - 1
    route_info = (
        f"{hops} road segment(s), ~{total_cost:.0f}s ({total_cost / 60:.1f} min) "
        f"estimated travel time, passing through {len(route_osmids)} intersections."
    )

    with driver.session() as session:
        advisories = get_route_advisories(session, route_osmids)

    return synthesise(llm, question, route_info, advisories)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

DEMO_QUERIES = [
    {
        "question":    "Any disruptions on my route from the station to Courtenay Place?",
        "start_spec":  "Lambton Quay / Bunny Street",
        "end_spec":    "Courtenay Place / Tory Street",
    },
    {
        "question":    "Is it safe to drive down Willis Street right now?",
        "start_spec":  "Willis Street / Lambton Quay",
        "end_spec":    "Willis Street / Dixon Street",
    },
    {
        "question":    "What should I expect on Cuba Street between Manners and Vivian?",
        "start_spec":  "Cuba Street / Manners Street",
        "end_spec":    "Cuba Street / Vivian Street",
    },
]


def run_demo(driver, llm):
    print("=" * 64)
    print("GraphRAG Demo — Route-aware advisory retrieval (Ollama)")
    print("=" * 64)

    with driver.session() as session:
        resolved = []
        for q in DEMO_QUERIES:
            src = resolve_location(session, q["start_spec"])
            tgt = resolve_location(session, q["end_spec"])
            resolved.append((src, tgt))

    for q, (src_osmid, tgt_osmid) in zip(DEMO_QUERIES, resolved):
        print(f"\nQ: {q['question']}")
        if not src_osmid or not tgt_osmid:
            print(f"  ⚠ Could not resolve '{q['start_spec']}' or '{q['end_spec']}' — "
                  "street names may differ in OSM data. Skipping.")
            continue
        print(f"  Route: osmid {src_osmid} → {tgt_osmid}", flush=True)
        answer = retrieve_and_answer(driver, llm, q["question"], src_osmid, tgt_osmid)
        print(f"A: {answer}")
        print("-" * 64)


# ---------------------------------------------------------------------------
# REPL
# ---------------------------------------------------------------------------

def repl(driver, llm):
    print("GraphRAG REPL")
    print("Resolve intersections by street name, e.g. 'Lambton Quay / Willis Street'")
    print("or a single street name for the busiest intersection on that street.")
    print("Type 'quit' to exit.\n")

    while True:
        try:
            start_spec = input("Start (street / street): ").strip()
            if start_spec.lower() in ("quit", "exit", "q"):
                break
            end_spec = input("End   (street / street): ").strip()
            question = input("Question               : ").strip()
            if not question:
                continue

            with driver.session() as session:
                src_osmid = resolve_location(session, start_spec)
                tgt_osmid = resolve_location(session, end_spec)

            if not src_osmid or not tgt_osmid:
                print("Could not locate one or both intersections. "
                      "Try different street names.\n")
                continue

            print(f"\n  Route: osmid {src_osmid} → {tgt_osmid}", flush=True)
            answer = retrieve_and_answer(driver, llm, question, src_osmid, tgt_osmid)
            print(f"\nA: {answer}\n")

        except KeyboardInterrupt:
            break

    print("Bye.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    llm = ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0,
    )
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        if "--repl" in sys.argv:
            repl(driver, llm)
        else:
            run_demo(driver, llm)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        driver.close()
