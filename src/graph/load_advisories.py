"""
Phase 6 — Load Advisories (src/load_advisories.py)

Inserts synthetic :Advisory nodes into Neo4j, embeds their text via Ollama's
nomic-embed-text model, and links each advisory to the :Intersection nodes
it affects via :AFFECTS_ROAD relationships.

Linking is done by graph topology: any intersection that sits on a road whose
name matches one of the advisory's street_names is linked. No geocoding needed.

Usage:
    python src/load_advisories.py

Environment (.env):
    NEO4J_URI           bolt://localhost:7687
    NEO4J_USER          neo4j
    NEO4J_PASSWORD      <your password>
    OLLAMA_BASE_URL     http://localhost:11434    (optional)
    OLLAMA_EMBED_MODEL  nomic-embed-text          (optional, must be 768-dim)
"""

import os
import sys

from dotenv import load_dotenv
from langchain_ollama import OllamaEmbeddings
from neo4j import GraphDatabase

import schema

load_dotenv()

NEO4J_URI          = os.getenv("NEO4J_URI",          "bolt://localhost:7687")
NEO4J_USER         = os.getenv("NEO4J_USER",         "neo4j")
NEO4J_PASSWORD     = os.getenv("NEO4J_PASSWORD",     "")
OLLAMA_BASE_URL    = os.getenv("OLLAMA_BASE_URL",    "http://localhost:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")

# ---------------------------------------------------------------------------
# Synthetic advisories — realistic Wellington CBD disruptions
# street_names must match OSM name values stored on :ROAD relationships
# ---------------------------------------------------------------------------
ADVISORIES = [
    {
        "id":           "adv-001",
        "title":        "Lambton Quay pipe replacement closure",
        "text":         (
            "Lambton Quay is closed between Willis Street and Panama Street "
            "for emergency water pipe replacement works. Northbound traffic is "
            "diverted via Featherston Street. Southbound access maintained via "
            "single contraflow lane. Expect significant delays 20–25 May 2026."
        ),
        "published":    "2026-05-19",
        "street_names": ["Lambton Quay", "Willis Street", "Panama Street"],
    },
    {
        "id":           "adv-002",
        "title":        "New 30 km/h speed limit on Courtenay Place",
        "text":         (
            "A permanent 30 km/h speed limit takes effect on Courtenay Place "
            "from 1 June 2026, replacing the previous 50 km/h limit. New road "
            "markings and signage are being installed this week. Temporary "
            "variable-speed signs are active during installation."
        ),
        "published":    "2026-05-20",
        "street_names": ["Courtenay Place"],
    },
    {
        "id":           "adv-003",
        "title":        "Temporary traffic lights at Cuba St / Vivian St",
        "text":         (
            "Temporary traffic lights have been installed at the Cuba Street "
            "and Vivian Street intersection to manage peak-hour congestion "
            "during the adjacent construction project. Expect 3–5 minute "
            "delays during morning (7–9am) and evening (4:30–6:30pm) peaks."
        ),
        "published":    "2026-05-20",
        "street_names": ["Cuba Street", "Vivian Street"],
    },
    {
        "id":           "adv-004",
        "title":        "Manners Street footpath reconstruction",
        "text":         (
            "Footpath reconstruction on Manners Street between Cuba Street "
            "and Dixon Street reduces the carriageway to one lane each "
            "direction. Works are scheduled weekdays 7am–6pm; completion "
            "is expected by end of May 2026."
        ),
        "published":    "2026-05-18",
        "street_names": ["Manners Street", "Cuba Street", "Dixon Street"],
    },
    {
        "id":           "adv-005",
        "title":        "Willis Street raised pedestrian crossing works",
        "text":         (
            "A new raised pedestrian crossing is being installed on Willis "
            "Street near Boulcott Street. A 10 km/h speed limit applies "
            "through the works zone. Expect lane restrictions and minor "
            "delays until 30 May 2026."
        ),
        "published":    "2026-05-17",
        "street_names": ["Willis Street", "Boulcott Street"],
    },
]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _upsert_advisory_and_link(tx, advisory_id, title, text, published, embedding, street_names):
    """Write the Advisory node and all AFFECTS_ROAD edges in one transaction."""
    tx.run(
        """
        MERGE (a:Advisory {id: $id})
        SET a.title     = $title,
            a.text      = $text,
            a.published = $published,
            a.embedding = $embedding
        """,
        id=advisory_id,
        title=title,
        text=text,
        published=published,
        embedding=embedding,
    )
    result = tx.run(
        """
        UNWIND $street_names AS sname
        MATCH (i:Intersection)-[r:ROAD]-()
        WHERE r.name = sname
        WITH DISTINCT i
        MATCH (adv:Advisory {id: $advisory_id})
        MERGE (adv)-[:AFFECTS_ROAD]->(i)
        RETURN count(i) AS linked
        """,
        street_names=street_names,
        advisory_id=advisory_id,
    )
    record = result.single()
    return record["linked"] if record else 0


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load(driver, embeddings: OllamaEmbeddings):
    # Ensure Advisory constraint + vector index exist
    with driver.session() as session:
        schema.apply(session)

    # Generate all embeddings in one batch call
    texts = [a["text"] for a in ADVISORIES]
    print(f"Generating embeddings via '{OLLAMA_EMBED_MODEL}' ({len(texts)} documents)…", flush=True)
    vectors = embeddings.embed_documents(texts)
    print(f"  Embedding dimension: {len(vectors[0])}")

    with driver.session() as session:
        for advisory, vector in zip(ADVISORIES, vectors):
            linked = session.execute_write(
                _upsert_advisory_and_link,
                advisory["id"],
                advisory["title"],
                advisory["text"],
                advisory["published"],
                vector,
                advisory["street_names"],
            )
            status = f"{linked} intersection(s) linked" if linked else "no intersections matched (street names may differ in OSM)"
            print(f"  {advisory['id']}: '{advisory['title']}' — {status}")

    print("\nVerification query:")
    print("  MATCH (a:Advisory) RETURN a.id, a.title, size(a.embedding) AS dims")
    print("  MATCH (a:Advisory)-[:AFFECTS_ROAD]->(i) RETURN a.id, count(i) AS intersections")


if __name__ == "__main__":
    embeddings = OllamaEmbeddings(
        model=OLLAMA_EMBED_MODEL,
        base_url=OLLAMA_BASE_URL,
    )
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        load(driver, embeddings)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        driver.close()
