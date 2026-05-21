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
| **Structural analytics** | Neo4j GDS (Betweenness, PageRank) | Identifies critical bottleneck intersections in the network |
| **Temporal memory** | Neo4j native datetime + :TrafficSnapshot | Time-series history enables rush-hour patterns and congestion duration |
| **Traffic signal control** | Approach sensors on :ROAD + :TrafficLight nodes | Demand-responsive green-time allocation from per-road sensor data |
| **High-resolution time-series** | InfluxDB 3 Core | Sub-second sensor writes, long-term retention, efficient aggregate queries |
| **Conversational analytics** | InfluxDB MCP Server + GitHub Copilot / Claude | Natural-language SQL queries against live traffic time-series |

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
ingest_to_neo4j.py        ← Phases 3–4 & 10
  273 :Intersection nodes
  539 :ROAD relationships
  273 :TrafficLight nodes (one per intersection, via [:HAS_LIGHT])
  schema: constraints, b-tree, text, spatial indexes
      │
      ├──────────────────────────────────────────┐
      ▼                                          ▼
load_advisories.py         ← Phase 6    kafka_producer.py   ← Phase 7 & 10
  5 :Advisory nodes                       reads 273 osmids + 539 road pairs
  768-dim nomic-embed-text vectors        emits JSON to 'sensor-readings'
  :AFFECTS_ROAD edges to                  and 'approach-sensors' topics
  affected :Intersections                 every 0.5 s                        │
      │                                          ▼
      │                                  kafka_consumer.py   ← Phase 7 & 10
      │                                    consumes 'sensor-readings':
      │                                      writes current_speed,
      │                                      current_flow, last_seen
      │                                      to :Intersection nodes
      │                                      → alerts → 'traffic-alerts'
      │                                    consumes 'approach-sensors':
      │                                      writes sensor_queue,
      │                                      sensor_arrival_rate,
      │                                      sensor_occupancy to :ROAD
      │                                          │
      └──────────────────────────────────────────┘
                    │
                    ▼
           Neo4j (bolt://localhost:7687)
           ┌───────────────────────────────────────────┐
           │  :Intersection  ──[:ROAD]──►  :Intersection│
           │       │  ▲                                 │
           │  [:HAS_LIGHT] [:AFFECTS_ROAD]              │
           │       │         │                          │
           │  :TrafficLight :Advisory (+embedding)      │
           └───────────────────────────────────────────┘
                    │
          ┌─────────┼──────────────────────────────────────┐
          ▼         ▼                      ▼               ▼
  text2cypher.py  graphrag_retriever.py  gds_analytics.py  traffic_history.py
  Phase 5         Phase 6               Phase 8           Phase 9
  NL → Cypher     Dijkstra route →      Betweenness       Rush-hour patterns
  via llama3.1    advisory retrieval     Centrality +      chronic congestion
                  → LLM                  PageRank          duration queries
                                                  │
                                                  ▼
                                    traffic_light_controller.py  ← Phase 10
                                      reads sensor_queue from inbound ROADs
                                      computes NS/EW green-time split
                                      writes phase + green_ns/ew to :TrafficLight
                                          │
                    ┌─────────────────────┘
                    │  (same Kafka topics)
                    ▼
          influx_consumer.py   ← Phase 11
            subscribes to sensor-readings + approach-sensors
            batches up to 100 pts or 1 s flush interval
            writes to InfluxDB 3 Core:
              traffic_sensor{intersection_osmid}   vehicle_count, avg_speed_kph
              approach_sensor{from_osmid,to_osmid} queue_length, arrival_rate, occupancy_pct
                    │
                    ▼
          InfluxDB 3 Core (http://localhost:8181)
          database: traffic
                    │
          ┌─────────┴──────────────────────┐
          ▼                                ▼
  Grafana dashboard              MCP Server (npx @influxdata/influxdb3-mcp-server)
  (http://localhost:3000)          │
  Infinity datasource →            ▼
  live SQL queries           GitHub Copilot / Claude Desktop
                             "Which intersections are congested right now?"
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

### Phase 8 — GDS Graph Analytics (`src/gds_analytics.py`)

Runs Neo4j Graph Data Science algorithms to score every intersection by structural importance:

- **Betweenness Centrality** → `criticality` property — how many shortest paths pass through this node. High-criticality intersections are network bottlenecks: if they fail, many routes are disrupted.
- **PageRank** → `pagerank` property — importance from a flow/connectivity perspective. Intersections fed by many well-connected roads rank highest.

Both scores are written directly onto `:Intersection` nodes, enabling combined structural + live-traffic queries.

Run it:
```bash
python src/gds_analytics.py           # compute & print top-10 summary
python src/gds_analytics.py --query   # also run insight queries
```

Key insight queries this enables:
```cypher
-- Critical bottlenecks that are CURRENTLY congested
MATCH (i:Intersection)
WHERE i.current_speed < 15 AND i.criticality > 0.01
RETURN i.osmid, i.criticality, i.pagerank, i.current_speed
ORDER BY i.criticality DESC
```

```cypher
-- Advisories hitting structurally important intersections
MATCH (a:Advisory)-[:AFFECTS_ROAD]->(i:Intersection)
WHERE i.criticality > 0.01
RETURN a.title, count(i) AS critical_nodes, round(avg(i.criticality), 4) AS avg_crit
ORDER BY avg_crit DESC
```

This turns the graph from a passive topology store into an **active risk model**: you can now prioritise alerts and maintenance based on structural importance, not just current congestion.

### Phase 9 — Temporal Traffic History (`src/traffic_history.py`)

Previously every Kafka reading *overwrote* `current_speed` — the graph had no memory. Phase 9 adds a `:TrafficSnapshot` node for every reading, linked to its intersection:

```
(:Intersection)-[:HAD_READING]->(:TrafficSnapshot {
    speed: 12.4, flow: 28,
    recorded_at: datetime("2026-05-21T08:32:11Z")
})
```

Neo4j's first-class `datetime` type and a range index on `recorded_at` make temporal queries efficient.

Run it:
```bash
python src/traffic_history.py              # all summaries
python src/traffic_history.py --morning    # 07:00–09:00 rush hour
python src/traffic_history.py --evening    # 16:30–18:30 rush hour
python src/traffic_history.py --chronic    # most-congested intersections
python src/traffic_history.py --duration   # how long congestion persists
```

Key insight queries this enables:
```cypher
-- Morning rush-hour: slowest intersections
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE time(s.recorded_at) >= time("07:00")
  AND time(s.recorded_at) <  time("09:00")
RETURN i.osmid, avg(s.speed) AS morning_avg
ORDER BY morning_avg ASC LIMIT 10
```

```cypher
-- Chronic congestion: intersections that are ALWAYS slow
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE s.speed < 15
RETURN i.osmid, count(s) AS congestion_events, avg(s.speed) AS avg_speed
ORDER BY congestion_events DESC LIMIT 10
```

```cypher
-- Combine temporal + structural: critical bottlenecks with chronic congestion
MATCH (i:Intersection)-[:HAD_READING]->(s:TrafficSnapshot)
WHERE s.speed < 15 AND i.criticality > 0.01
RETURN i.osmid, i.criticality, count(s) AS events, avg(s.speed) AS avg_speed
ORDER BY i.criticality DESC
```

This transforms the graph from a *current-state snapshot* into a **time-aware urban memory** — enabling pattern detection, anomaly baselines, and predictive queries with pure Cypher.

### Phase 10 — Demand-Responsive Traffic Light Control (`src/traffic_light_controller.py`)

Each intersection is given a `:TrafficLight` node (created during ingest). Each inbound `:ROAD` relationship carries approach-sensor properties updated in real time by the Kafka consumer:

- `sensor_queue` — vehicles stopped at the approach
- `sensor_arrival_rate` — vehicles/min arriving
- `sensor_occupancy` — inductive loop occupancy %
- `sensor_ts` — last reading timestamp

The traffic-light controller runs a periodic cycle:
1. Gathers sensor data from all inbound `:ROAD` relationships
2. Classifies each approach as **NS** or **EW** based on geographic bearing
3. Computes aggregate demand per direction: `demand = queue + 2 × arrival_rate`
4. Allocates green-time proportionally (min 20% per direction)
5. Writes `green_ns`, `green_ew`, `current_phase` back to `:TrafficLight`

Run it:
```bash
python src/traffic_light_controller.py              # continuous (every 10s)
python src/traffic_light_controller.py --once       # single pass
python src/traffic_light_controller.py --interval 5 # 5-second cycle
```

Key queries this enables:
```cypher
-- Intersections where traffic lights favour NS direction
MATCH (i:Intersection)-[:HAS_LIGHT]->(t:TrafficLight)
WHERE t.green_ns > 0.6
RETURN i.osmid, t.green_ns, t.green_ew, t.current_phase
ORDER BY t.green_ns DESC LIMIT 10
```

```cypher
-- Most congested approaches (highest queue lengths)
MATCH (a:Intersection)-[r:ROAD]->(b:Intersection)
WHERE r.sensor_queue IS NOT NULL
RETURN a.osmid AS from, b.osmid AS to, r.name,
       r.sensor_queue, r.sensor_arrival_rate
ORDER BY r.sensor_queue DESC LIMIT 10
```

```cypher
-- Traffic lights on critical bottlenecks that are currently overloaded
MATCH (a:Intersection)-[r:ROAD]->(b:Intersection)-[:HAS_LIGHT]->(t:TrafficLight)
WHERE b.criticality > 0.01 AND r.sensor_queue > 20
RETURN b.osmid, b.criticality, r.sensor_queue,
       t.current_phase, t.green_ns
ORDER BY b.criticality DESC
```

This closes the loop from **sensing** (approach sensors on roads) through **reasoning** (demand computation) to **actuation** (traffic light phase allocation) — all within the graph.

### Phase 11 — InfluxDB Time-Series + MCP Conversational Analytics (`src/influxdb/influx_consumer.py`)

While Neo4j keeps *current state* and *graph-structured history*, InfluxDB 3 Core keeps the **raw high-resolution time-series** — every single sensor reading at full fidelity, queryable via SQL with sub-second precision.

`influx_consumer.py` subscribes to both Kafka topics in parallel and writes two measurements:

| Measurement | Tags | Fields |
|---|---|---|
| `traffic_sensor` | `intersection_osmid` | `vehicle_count`, `avg_speed_kph` |
| `approach_sensor` | `from_osmid`, `to_osmid` | `queue_length`, `arrival_rate`, `occupancy_pct` |

Points are batched (up to 100 points or 1 s flush interval) and written with the original sensor timestamp so that historical queries reflect actual event time, not ingestion lag.

Run it:
```bash
python src/influxdb/influx_consumer.py          # runs until Ctrl-C
python src/influxdb/influx_consumer.py --once   # consume until idle, then exit
```

Or start the full pipeline including InfluxDB consumer:
```bash
./start_pipeline.sh
```

**Grafana dashboard** — open `http://localhost:3000` (admin / admin).  The InfluxDB3-Traffic datasource is pre-provisioned via `grafana/provisioning/`.  Use the Infinity datasource panel to run any SQL query below directly in the UI.

**MCP Conversational Interface** — the InfluxDB MCP Server is pre-configured in `.vscode/mcp.json`.  Restart VS Code after `docker compose up -d` and ask Copilot:

> "Which intersections have had average speed below 15 km/h in the last 10 minutes?"
> "What was the peak queue length on any approach in the last hour?"
> "Compare vehicle throughput between intersection 268939537 and the network average today."

Key SQL queries this enables:
```sql
-- Network congestion pulse (last 5 minutes)
SELECT
    COUNT(DISTINCT intersection_osmid)              AS monitored,
    COUNT(CASE WHEN avg_speed_kph < 15 THEN 1 END) AS congested_readings,
    ROUND(AVG(avg_speed_kph), 1)                    AS network_speed_kph
FROM traffic_sensor
WHERE time > now() - INTERVAL '5 minutes'
```

```sql
-- Top 10 worst approaches right now
SELECT from_osmid, to_osmid,
       ROUND(AVG(queue_length), 1)   AS avg_queue,
       ROUND(AVG(occupancy_pct), 1)  AS avg_occupancy
FROM approach_sensor
WHERE time > now() - INTERVAL '2 minutes'
GROUP BY from_osmid, to_osmid
ORDER BY avg_queue DESC
LIMIT 10
```

```sql
-- One-minute bucketed speed trend for a specific intersection
SELECT
    date_trunc('minute', time) AS bucket,
    AVG(avg_speed_kph)         AS avg_speed,
    SUM(vehicle_count)         AS total_vehicles
FROM traffic_sensor
WHERE intersection_osmid = '268939537'
  AND time > now() - INTERVAL '1 hour'
GROUP BY bucket
ORDER BY bucket
```

This adds a **purpose-built time-series layer** alongside the graph: InfluxDB handles million-row aggregate scans efficiently where Neo4j excels at graph traversal, and the MCP server bridges the gap to conversational AI without any custom prompt engineering.

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
| Route optimisation | ✅ Done — `gds_analytics.py` writes `criticality` + `pagerank` to every intersection |
| Public alerts API | Expose the `traffic-alerts` Kafka topic via a WebSocket endpoint |
| Historical replay | \u2705 Done — `kafka_consumer.py` writes `:TrafficSnapshot` nodes with native `datetime`; `traffic_history.py` analyses patterns |
| Traffic signal control | ✅ Done — `traffic_light_controller.py` reads approach sensors, computes demand-responsive green splits |
| High-resolution time-series | ✅ Done — `influx_consumer.py` writes both Kafka topics to InfluxDB 3 Core at full resolution |
| Conversational time-series queries | ✅ Done — InfluxDB MCP Server wired into `.vscode/mcp.json`; ask Copilot directly |
| Multi-modal transport | Add `:BusRoute`, `:TrainLine` nodes linked to `:Intersection` by proximity |

---

## References

- [Neo4j Graph Data Science](https://neo4j.com/product/graph-data-science/) — GDS library used for Betweenness Centrality and PageRank analytics
- [Text2Cypher Guide](https://neo4j.com/blog/genai/text2cypher-guide/) — Natural-language to Cypher query generation patterns
- [GraphRAG with Python](https://neo4j.com/developer/genai-ecosystem/graphrag-python/) — Graph-augmented retrieval-augmented generation with Neo4j and LangChain
- [InfluxDB 3 Core](https://www.influxdata.com/products/influxdb/) — Open-source time-series database used for sensor data storage
- [InfluxDB MCP Server](https://github.com/influxdata/influxdb3-mcp-server) — MCP server for natural-language SQL queries against InfluxDB 3
