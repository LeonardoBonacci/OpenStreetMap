# Wellington CBD Traffic Intelligence — Showcase

A working smart-city prototype built on top of real OpenStreetMap data, a graph database, a streaming message bus, and a local LLM. Every component runs on a single laptop with no cloud dependencies.

---

## What This Demonstrates

The project stitches together five concerns that any real urban-intelligence platform must solve:

| Concern | Technology | This project's answer |
|---|---|---|
| **Physical world model** | OSMNx + Neo4j | Real Wellington CBD road topology as a property graph |
| **Streaming sensor data** | Apache Kafka | Simulated IoT readings arrive as a continuous event stream |
| **Live state maintenance** | Kafka consumer + Neo4j | Graph nodes are mutated in place as events arrive |
| **Contextual knowledge** | Advisory nodes + vector index | Human-authored disruption notices linked to the graph by topology |
| **Natural language interface** | Ollama (llama3.1) + LangChain | Both Text→Cypher analytics and GraphRAG route Q&A |

None of these is novel in isolation. The novelty is their **composition**: every query can simultaneously touch static geometry, live traffic state, and semantic advisory text, all within the same graph traversal.

---

## Architecture

```
OSM (Overpass API)
      │
      ▼
download_network.py       ← Phase 2
  osmnx.graph_from_bbox()
  + edge speed / travel-time enrichment
  → data/wellington_cbd.graphml (cache)
      │
      ▼
ingest_to_neo4j.py        ← Phases 3–4
  273 :Intersection nodes
  539 :ROAD relationships
  schema: constraints, b-tree, text, spatial indexes
      │
      ├──────────────────────────────────────────┐
      ▼                                          ▼
load_advisories.py         ← Phase 6    kafka_producer.py   ← Phase 7
  5 :Advisory nodes                       reads 273 osmids from Neo4j
  768-dim nomic-embed-text vectors        emits JSON to 'sensor-readings'
  :AFFECTS_ROAD edges to                  topic every 0.5 s
  affected :Intersections                        │
      │                                          ▼
      │                                  kafka_consumer.py   ← Phase 7
      │                                    consumes readings
      │                                    writes current_speed,
      │                                    current_flow, last_seen
      │                                    to :Intersection nodes
      │                                    → alerts → 'traffic-alerts'
      │                                          │
      └──────────────────────────────────────────┘
                    │
                    ▼
           Neo4j (bolt://localhost:7687)
           ┌───────────────────────────────────────────┐
           │  :Intersection  ──[:ROAD]──►  :Intersection│
           │       ▲                                    │
           │  [:AFFECTS_ROAD]                           │
           │       │                                    │
           │  :Advisory (+ 768-dim embedding vector)    │
           └───────────────────────────────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
  text2cypher.py        graphrag_retriever.py
  Phase 5               Phase 6
  NL → Cypher           Dijkstra route → advisory retrieval → LLM
  via llama3.1          route-aware answers via llama3.1
```

---

## How the Data Pipeline Works

### Phase 2 — Road Network Download (`src/download_network.py`)

OSMNx queries the Overpass API for every drivable road inside a bounding box:

```
north=-41.276, south=-41.295, east=174.785, west=174.770
```

It then enriches each edge with:
- `speed_kph` — imputed from OSM `maxspeed` tags, or a 50 km/h fallback
- `travel_time` — `length / (speed_kph / 3.6)` in seconds

The result is cached to `data/wellington_cbd.graphml`. Re-runs skip the download.

### Phases 3–4 — Graph Schema + Ingest (`src/schema.py`, `src/ingest_to_neo4j.py`)

The GraphML is read back and batch-loaded into Neo4j:

- **273 `:Intersection` nodes** — one per OSM node id, carrying `osmid`, `lat`, `lon`, `street_count`
- **539 `:ROAD` relationships** — directed edges carrying `name`, `highway`, `length`, `speed_kph`, `travel_time`, `oneway`, `reversed`

Schema applied:

```cypher
CREATE CONSTRAINT intersection_osmid IF NOT EXISTS
  FOR (n:Intersection) REQUIRE n.osmid IS UNIQUE

CREATE INDEX intersection_location IF NOT EXISTS
  FOR (n:Intersection) ON (n.lat, n.lon)

CREATE INDEX road_highway IF NOT EXISTS
  FOR ()-[r:ROAD]-() ON (r.highway)

CREATE TEXT INDEX road_name IF NOT EXISTS
  FOR ()-[r:ROAD]-() ON (r.name)
```

### Phase 5 — Natural Language Analytics (`src/text2cypher.py`)

A LangChain `GraphCypherQAChain` translates free-text questions into Cypher using a locally-running `llama3.1:latest` model via Ollama. The chain:

1. Introspects the Neo4j schema automatically
2. Prompts the LLM with the schema + question → receives raw Cypher
3. Executes the Cypher and feeds results back to the LLM for a plain-English answer

Run interactively:
```bash
python src/text2cypher.py --repl
```

Example questions it handles:
- *"Which road type has the most segments?"*
- *"What percentage of roads are one-way?"*
- *"Which intersection connects the most streets?"*
- *"Which named street has the longest total length?"*

### Phase 6 — Advisory Nodes + GraphRAG (`src/load_advisories.py`, `src/graphrag_retriever.py`)

`load_advisories.py` inserts 5 synthetic Wellington CBD disruption notices as `:Advisory` nodes. Each advisory:
- Has its full text embedded by `nomic-embed-text` (768 dimensions) via Ollama
- Is stored in a Neo4j **vector index** for semantic similarity search
- Is linked to every `:Intersection` that sits on any of its named streets via `:AFFECTS_ROAD`

The linking query — purely graph topology, no geocoding:
```cypher
UNWIND $street_names AS sname
MATCH (i:Intersection)-[r:ROAD]-()
WHERE r.name = sname
WITH DISTINCT i
MATCH (adv:Advisory {id: $advisory_id})
MERGE (adv)-[:AFFECTS_ROAD]->(i)
```

`graphrag_retriever.py` answers route questions in three steps:
1. Resolve start/end street intersections to `osmid` values
2. Run GDS Dijkstra shortest path weighted by `travel_time`
3. Fetch all `:Advisory` nodes linked to intersections *on that route* — not by keyword, but by graph proximity
4. Feed the route summary + advisory texts to the LLM

Run interactively:
```bash
python src/graphrag_retriever.py --repl
# Start: Lambton Quay / Willis Street
# End  : Courtenay Place / Tory Street
# Q    : Any disruptions on this route?
```

### Phase 7 — Live Traffic Stream (`src/kafka_producer.py`, `src/kafka_consumer.py`)

**Producer** — simulates IoT sensors at every intersection:
- Picks a random `osmid` from Neo4j every 0.5 seconds
- ~95 % normal flow: `avg_speed_kph` 20–70, `vehicle_count` 0–30
- ~5 % congestion spike: `avg_speed_kph` 2–15, `vehicle_count` 30–60
- Publishes JSON to the `sensor-readings` Kafka topic

**Consumer** — processes the stream in real time:
- Writes `current_speed`, `current_flow`, `last_seen` directly onto each `:Intersection` node
- If `avg_speed_kph < 15`, publishes a congestion alert to `traffic-alerts` topic

---

## Observing Live Changes in Neo4j

Open the browser at **http://localhost:7474** and log in with your `.env` credentials.

### See everything
```cypher
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 300
```

### All intersections with live traffic state
```cypher
MATCH (i:Intersection)
WHERE i.current_speed IS NOT NULL
RETURN i.osmid, i.current_speed, i.current_flow, i.last_seen
ORDER BY i.current_speed ASC
```

### Currently congested intersections (speed < 15 km/h)
```cypher
MATCH (i:Intersection)
WHERE i.current_speed < 15
RETURN i.osmid, i.current_speed, i.current_flow, i.last_seen
ORDER BY i.current_speed ASC
```

### Advisories and how many intersections they affect
```cypher
MATCH (a:Advisory)-[:AFFECTS_ROAD]->(i:Intersection)
RETURN a.id, a.title, count(i) AS intersections
ORDER BY a.id
```

### The most powerful query — advisories affecting congested intersections right now
```cypher
MATCH (a:Advisory)-[:AFFECTS_ROAD]->(i:Intersection)
WHERE i.current_speed IS NOT NULL AND i.current_speed < 15
RETURN a.title, i.osmid, round(i.current_speed, 1) AS speed_kph, i.last_seen
ORDER BY i.current_speed ASC
```
This is what makes the graph approach valuable: a single traversal crosses the static road topology, the live sensor state, and the human-authored advisory text simultaneously. A relational join or a pure vector search cannot do this in one step.

### Inspect the full graph schema at runtime
```cypher
CALL apoc.meta.stats() YIELD labels, relTypesCount
RETURN labels, relTypesCount
```

### Watch a specific intersection update in near-real time
Pick any `osmid` from a previous query, then re-run every few seconds:
```cypher
MATCH (i:Intersection {osmid: 268939537})
RETURN i.current_speed, i.current_flow, i.last_seen
```

---

## Why This Is a Good Baseline for Smart Cities

### 1. The graph is the right data structure

Roads, intersections, routes, and zones are all fundamentally relational. A graph database stores this naturally — no foreign-key joins, no spatial extension required for topology. The GDS Dijkstra shortest-path query runs over the same data structure the advisory links live in.

Real smart-city extensions that map directly onto this model:
- Public transit stops as `:Stop` nodes linked to `:Intersection` by `:NEAR`
- Traffic light phases as `:Signal` nodes on `:ROAD` edges
- Parking zones, emergency routes, school zones as labelled subgraphs

### 2. Streaming state on top of a static model

The most common smart-city mistake is treating live data and map data as separate systems. This project shows they can coexist on the same nodes. The static `speed_kph` (from OSM) and the live `current_speed` (from Kafka) sit on the same `:Intersection` — allowing queries like *"where is traffic slower than the posted speed limit right now?"*:

```cypher
MATCH (a:Intersection)-[r:ROAD]->(b:Intersection)
WHERE a.current_speed IS NOT NULL
  AND a.current_speed < r.speed_kph * 0.5
RETURN r.name, a.osmid, a.current_speed, r.speed_kph
ORDER BY a.current_speed ASC
LIMIT 20
```

### 3. Contextual knowledge (advisories) is first-class

Advisory text is not stored in a separate CMS. It lives in the same graph, linked by topology. This means a route query naturally surfaces relevant disruptions without a keyword search — the LLM receives precisely the advisories that affect the computed path, nothing else. This is the core idea behind **GraphRAG**: use the graph's structure to filter context before sending it to the LLM, rather than relying purely on vector similarity.

### 4. The LLM layer is thin and replaceable

Neither `text2cypher.py` nor `graphrag_retriever.py` hard-codes city-specific logic. The LLM generates or summarises; the graph does the reasoning. Swapping `llama3.1` for a different model, or replacing Ollama with an API, requires changing one environment variable.

### 5. Everything is local and reproducible

| Component | Technology | Why it matters |
|---|---|---|
| Graph DB | Neo4j 5 Community (Docker) | Runs on a laptop, no cloud billing |
| Message bus | Apache Kafka (Docker, KRaft mode) | No ZooKeeper, single-node, disposable |
| Embeddings | Ollama `nomic-embed-text` | 768-dim, runs on CPU, offline |
| LLM | Ollama `llama3.1:latest` | No API key, no data leaves the machine |
| Map data | OpenStreetMap via Overpass | Free, global, community-maintained |

A city council, research lab, or startup can stand this up in an afternoon and iterate on the data model without committing to any vendor.

### 6. Natural extension points

| What to add | How |
|---|---|
| Real sensor feeds | Replace the Kafka producer with an MQTT bridge or HTTP webhook |
| More city districts | Change the bounding box in `download_network.py` |
| Incident detection | Add a second consumer that writes `:Incident` nodes when thresholds are exceeded |
| Route optimisation | Add GDS betweenness centrality to identify critical intersections |
| Public alerts API | Expose the `traffic-alerts` Kafka topic via a WebSocket endpoint |
| Historical replay | Persist consumer events to a time-series node `(:Reading {ts, speed, flow})-[:AT]->(:Intersection)` |
| Multi-modal transport | Add `:BusRoute`, `:TrainLine` nodes linked to `:Intersection` by proximity |
