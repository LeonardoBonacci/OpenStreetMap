# Wellington CBD Traffic Intelligence

A progressive experiment building a smart-city graph pipeline on top of the Wellington CBD road network:

| Phase | What | Stack |
|---|---|---|
| **1–4** | Road graph → Neo4j | OSMNx · Neo4j 5 · GDS |
| **5** | Natural language analytics | LangChain · `GraphCypherQAChain` |
| **6** | Route-aware document Q&A | GraphRAG · Neo4j Vector |
| **7** | Live traffic simulation | Kafka · stream processor |

Each phase builds on the same Neo4j instance and the same `docker-compose.yml`.

---

## Quick Start (Phases 1–4)

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Create .env with Neo4j credentials
cp .env.example .env   # or create manually — see below

# 3. Start Neo4j (downloads GDS plugin automatically)
docker compose up -d

# 4. Download and cache the Wellington CBD road network
python src/download_network.py

# 5. Ingest cached graph into Neo4j
python src/ingest_to_neo4j.py
```

---

## Phase 1 — Project Scaffolding

1. `requirements.txt` — `osmnx>=2.0`, `neo4j>=5.0`, `python-dotenv`, `shapely`
2. `docker-compose.yml` — `neo4j:5-community` image, `NEO4J_PLUGINS: '["graph-data-science"]'` env var (auto-downloads GDS Community JAR), ports 7474/7687, `./data` and `./logs` volume mounts, 2 GB heap
3. `.env` — `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` (gitignored)
4. `pip install -r requirements.txt` *(depends on step 1)*

---

## Phase 2 — Download & Enrich (`src/download_network.py`)

5. Bounding box for Wellington CBD (~1 km × 1 km): `north=-41.276, south=-41.295, east=174.785, west=174.770`
6. `ox.graph_from_bbox((west, south, east, north), network_type="drive")` → `networkx.MultiDiGraph`
7. `ox.add_edge_speeds(G, fallback=50)` → imputes `speed_kph` on every edge (auto-converts mph → kph)
8. `ox.add_edge_travel_times(G)` → adds `travel_time` (seconds) per edge
9. Cache to `data/wellington_cbd.graphml` via `ox.save_graphml` — guarded so re-runs skip the download

---

## Phase 3 — Neo4j Schema (`src/schema.py`) — *depends on Docker running*

Applied automatically by `ingest_to_neo4j.py`. Statements:

**Constraint**
```cypher
CREATE CONSTRAINT intersection_osmid IF NOT EXISTS
  FOR (n:Intersection) REQUIRE n.osmid IS UNIQUE
```

**Indexes**
```cypher
CREATE INDEX intersection_location IF NOT EXISTS
  FOR (n:Intersection) ON (n.lat, n.lon)

CREATE INDEX road_highway IF NOT EXISTS
  FOR ()-[r:ROAD]-() ON (r.highway)

CREATE TEXT INDEX road_name IF NOT EXISTS
  FOR ()-[r:ROAD]-() ON (r.name)
```

---

## Phase 4 — Batch Ingest (`src/ingest_to_neo4j.py`) — *depends on Phase 2 + 3*

Clears any existing graph, applies schema, then loads in batches of 500.

**Nodes** — from `G.nodes(data=True)`:

| Property | Type | Source |
|---|---|---|
| `osmid` | INTEGER | OSM node ID (unique key) |
| `lat` | FLOAT | WGS-84 latitude (y) |
| `lon` | FLOAT | WGS-84 longitude (x) |
| `street_count` | INTEGER | edges meeting at node |

```cypher
UNWIND $batch AS n
MERGE (i:Intersection {osmid: n.osmid})
SET i.lat = n.lat, i.lon = n.lon, i.street_count = n.street_count
```

**Relationships** (`:ROAD`) — from `G.edges(data=True)`:

| Property | Type | Notes |
|---|---|---|
| `osmid` | INTEGER | OSM way ID; list → take first |
| `name` | STRING | street name |
| `highway` | STRING | OSM highway tag |
| `maxspeed` | STRING | posted speed limit string |
| `length` | FLOAT | metres |
| `speed_kph` | FLOAT | inferred/posted speed |
| `travel_time` | FLOAT | seconds |
| `oneway` | BOOLEAN | |
| `reversed` | BOOLEAN | edge reversed during simplification |

```cypher
UNWIND $batch AS e
MATCH (a:Intersection {osmid: e.u})
MATCH (b:Intersection {osmid: e.v})
CREATE (a)-[r:ROAD]->(b)
SET r.osmid = e.osmid, r.name = e.name, r.highway = e.highway,
    r.maxspeed = e.maxspeed, r.length = e.length,
    r.speed_kph = e.speed_kph, r.travel_time = e.travel_time,
    r.oneway = e.oneway, r.reversed = e.reversed
```

---

## Phase 5 — Text2Cypher (`src/text2cypher.py`) — *depends on Phase 4*

Natural language interface over the static road graph. A user asks a plain-English question; LangChain generates and executes the Cypher, then returns a natural language answer.

**Example questions:**
- *"Which road type has the most segments?"* → aggregates on `r.highway`
- *"What percentage of roads are one-way?"* → filters on `r.oneway`
- *"Which intersection connects the most streets?"* → orders by `i.street_count DESC`
- *"Which named street has the longest total length?"* → groups by `r.name`, sums `r.length`
- *"How many dead-end intersections are there?"* → filters `i.street_count = 1`

**Stack:** `langchain-neo4j` (`Neo4jGraph` + `GraphCypherQAChain`), `langchain-ollama` (`ChatOllama`)

`Neo4jGraph` auto-introspects the schema — no manual description needed. `verbose=True` prints generated Cypher alongside the answer for easy validation.

**Run:**
```bash
python src/text2cypher.py          # 5 demo questions
python src/text2cypher.py --repl   # interactive prompt
```

**Optional `.env` overrides:**
```
OLLAMA_MODEL=llama3.1:latest
OLLAMA_BASE_URL=http://localhost:11434
```

---

## Phase 6 — GraphRAG (`src/load_advisories.py` + `src/graphrag_retriever.py`) — *depends on Phase 5*

Adds synthetic traffic advisory documents as `:Advisory` nodes linked to the intersections they mention. Q&A can now blend graph topology with free-text knowledge.

**New graph elements:**
```
(:Advisory {id, title, text, published, embedding})
  -[:AFFECTS_ROAD]->(:Intersection)
```

**Example advisories (synthetic):**
- *"Lambton Quay closed between Willis St and Panama St for pipe replacement, 20–25 May"*
- *"New 30 km/h speed limit on Courtenay Place effective 1 June"*
- *"Temporary traffic lights at Cuba St / Vivian St intersection"*
- *"Manners Street footpath reconstruction reduces carriageway to one lane"*
- *"Willis Street raised pedestrian crossing works near Boulcott Street"*

**Why GraphRAG beats plain RAG here:**
1. User asks: *"Any disruptions on my route from the station to Courtenay Place?"*
2. GDS Dijkstra finds the route intersections
3. Advisories linked to intersections **along that path** are retrieved — not just keyword-matched docs
4. LLM synthesises a route-aware answer

The graph topology acts as a retrieval filter. A vector-only RAG has no concept of spatial adjacency.

**New schema additions** (appended to `src/schema.py`):
```cypher
CREATE CONSTRAINT advisory_id IF NOT EXISTS
  FOR (a:Advisory) REQUIRE a.id IS UNIQUE

-- 768 dimensions matches nomic-embed-text (local Ollama model)
CREATE VECTOR INDEX advisory_text IF NOT EXISTS
  FOR (a:Advisory) ON (a.embedding)
  OPTIONS {indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'cosine'}}
```

**Stack:** `langchain-ollama` (`OllamaEmbeddings` for 768-dim `nomic-embed-text` vectors, `ChatOllama` for synthesis)

**Run:**
```bash
# Pull the embedding model (one-time)
ollama pull nomic-embed-text

# Load advisory nodes + embeddings + AFFECTS_ROAD links
python src/load_advisories.py

# Run 3 demo route queries
python src/graphrag_retriever.py

# Interactive mode: enter start/end streets and a question
python src/graphrag_retriever.py --repl
```

REPL input format:
```
Start (street / street): Lambton Quay / Willis Street
End   (street / street): Courtenay Place / Tory Street
Question               : Any disruptions on this route?
```

**Optional `.env` overrides:**
```
OLLAMA_EMBED_MODEL=nomic-embed-text     # must be 768-dim; drop+recreate index if changed
OLLAMA_MODEL=llama3.1:latest
OLLAMA_BASE_URL=http://localhost:11434
```

---

## Phase 7 — Kafka Streaming (`src/kafka_producer.py` + `src/kafka_consumer.py`) — *depends on Phase 4*

Simulates IoT traffic sensors at intersections. The consumer writes live state back into Neo4j; Text2Cypher and GraphRAG queries automatically reflect real-time conditions.

**Topics:**

| Topic | Direction | Payload |
|---|---|---|
| `sensor-readings` | producer → consumer | `{intersection_osmid, vehicle_count, avg_speed_kph, ts}` |
| `traffic-alerts` | consumer → downstream | `{type, osmid, street, speed_kph, ts}` |

**Consumer logic:**
1. Consume from `sensor-readings`
2. Write live state to Neo4j:
   ```cypher
   MATCH (i:Intersection {osmid: $osmid})
   SET i.current_flow  = $vehicle_count,
       i.current_speed = $avg_speed_kph,
       i.last_seen     = $ts
   ```
3. If `avg_speed_kph < 15` → look up the street name at that intersection, produce a `CONGESTION` alert (with street name) to `traffic-alerts`

**Producer:** picks random intersections, generates plausible readings with ~5 % congestion spikes. `--count N` sends exactly N messages then exits.

**Text2Cypher on live data:** *"Which intersections are currently congested?"* → `WHERE i.current_speed < 15`

**GraphRAG on live data:** *"Is there a known reason for congestion on Willis Street?"* → retrieves linked advisories + checks `i.current_speed`

**Run:**
```bash
# Terminal 1 — start consumer
python src/kafka_consumer.py

# Terminal 2 — stream sensor readings
python src/kafka_producer.py              # continuous, Ctrl-C to stop
python src/kafka_producer.py --count 50  # exactly 50 messages
```

**Optional `.env` overrides:**
```
KAFKA_BOOTSTRAP=localhost:9092
CONSUMER_GROUP=traffic-consumer
PRODUCER_DELAY=0.5
```

**Docker service** (`docker-compose.yml`) — KRaft mode, no ZooKeeper:
```yaml
  kafka:
    image: apache/kafka:3.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qg
```

---

## Project Structure (end state)

```
.
├── .env                          # Neo4j + OpenAI credentials (gitignored)
├── docker-compose.yml            # Neo4j 5 + GDS + Kafka
├── requirements.txt
├── data/
│   └── wellington_cbd.graphml   # Cached OSMNx graph (generated)
└── src/
    ├── download_network.py       # Phase 2: OSMNx download, enrich, cache
    ├── schema.py                 # Phase 3: constraints + indexes (inc. vector)
    ├── ingest_to_neo4j.py        # Phase 4: batched node/rel load
    ├── text2cypher.py            # Phase 5: LangChain GraphCypherQAChain
    ├── load_advisories.py        # Phase 6: insert synthetic advisory docs
    ├── graphrag_retriever.py     # Phase 6: path-aware retrieval chain
    ├── kafka_producer.py         # Phase 7: simulated sensor readings
    └── kafka_consumer.py         # Phase 7: graph updates + alert emission
```

---

## Verification Checklist

**Phases 1–4**
1. `docker compose up -d` → open http://localhost:7474, confirm GDS shows in `:sysinfo`
2. `python src/download_network.py` → confirm `data/wellington_cbd.graphml` created; check printed node/edge counts
3. `python src/ingest_to_neo4j.py` → `MATCH (i:Intersection) RETURN count(i)` matches node count
4. `MATCH ()-[r:ROAD]->() RETURN count(r)` matches edge count

**Phase 5**
5. `python src/text2cypher.py` → ask *"Which intersection connects the most streets?"* — confirm Cypher printed and answer returned

**Phase 6**
6. `python src/load_advisories.py` → `MATCH (a:Advisory) RETURN count(a)` returns 5; each advisory shows `size(a.embedding) = 768`
7. `python src/graphrag_retriever.py` → three demo queries print a route osmid pair, then an LLM answer mentioning relevant advisory titles

**Phase 7**
8. `docker compose up -d` → both `neo4j-road-net` and `kafka-traffic` containers running
9. Start consumer (`python src/kafka_consumer.py`), then producer (`--count 50`) — consumer prints `[OK]` and `[ALERT]` lines
10. `MATCH (i:Intersection) WHERE i.current_speed IS NOT NULL RETURN count(i)` > 0 in Neo4j Browser
11. Congestion alert lines show resolved street names (e.g. `[ALERT] CONGESTION on Lambton Quay`)

---

## Decisions & Scope

- **Wellington CBD only** — compact ~1 km × 1 km bbox; expand by adjusting `BBOX` in `download_network.py`
- **Bounding box over place query** — more reliable than a place-name geocode for a compact area
- **GDS via Docker env var** — `NEO4J_PLUGINS` auto-downloads the Community JAR; no manual placement
- **Edge geometry skipped** — keeps the model simple; add as WKT string later if spatial queries are needed
- **Enrichment in Python** — `speed_kph` and `travel_time` computed before import; cheaper than post-load compute
- **Advisories are synthetic** — no real data source required; realistic enough to demo GraphRAG spatial retrieval
- **Embeddings via Ollama** — `nomic-embed-text` (768 dims) keeps everything local; no OpenAI key needed
- **GDS path yield** — `nodes(path)` used instead of `gds.util.asNode()` — the latter is sandboxed by default in Neo4j Community
- **Kafka KRaft mode** — `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` must explicitly map the `CONTROLLER` listener to `PLAINTEXT`; omitting it causes broker startup failure
- **Consumer `auto_offset_reset=latest`** — start consumer before producer to avoid missing messages; use `earliest` if replaying history
- **Kafka in KRaft mode** — no ZooKeeper dependency; single-node, single-partition, sufficient for local simulation
- **Credentials in `.env`** — loaded via `python-dotenv`, never hardcoded
