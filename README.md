# OpenStreetMap — Auckland CBD Road Network → Neo4j

Download the Auckland CBD drivable road graph via OSMNx, enrich it with speeds and travel times, then batch-load intersections (nodes) and road segments (weighted relationships) into a local Neo4j Docker instance with the GDS plugin. Deliverable is 3 Python scripts + docker-compose, ending with working Dijkstra shortest-path queries.

---

## Phase 1 — Project Scaffolding

1. `requirements.txt` — `osmnx>=2.0`, `neo4j>=5.0`, `python-dotenv`, `shapely`
2. `docker-compose.yml` — `neo4j:5-community` image, `NEO4J_PLUGINS: '["graph-data-science"]'` env var (auto-downloads GDS Community JAR), ports 7474/7687, `./data` and `./logs` volume mounts
3. `.env` — `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` (gitignored)
4. `python -m venv .venv && pip install -r requirements.txt` *(depends on step 1)*

---

## Phase 2 — Download & Enrich (`src/download_network.py`)

5. `ox.graph_from_place("Auckland CBD, Auckland, New Zealand", network_type="drive")` → `networkx.MultiDiGraph`
6. `ox.add_edge_speeds(G, fallback=50)` → imputes `speed_kph` on every edge (auto-converts mph → kph)
7. `ox.add_edge_travel_times(G)` → adds `travel_time` (seconds) per edge
8. Cache to `data/auckland_cbd.graphml` via `ox.save_graphml` — guarded so re-runs skip the download

---

## Phase 3 — Neo4j Schema (`src/load_neo4j.py`) — *depends on Docker running*

9. Uniqueness constraint:
   ```cypher
   CREATE CONSTRAINT intersection_osmid IF NOT EXISTS
     FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE;
   ```
10. Location index:
    ```cypher
    CREATE INDEX intersection_location IF NOT EXISTS
      FOR (i:Intersection) ON (i.latitude, i.longitude);
    ```

---

## Phase 4 — Batch Load Nodes — *depends on Phase 3*

11. Iterate `G.nodes(data=True)` → dicts with `{osmid, latitude (y), longitude (x), street_count}`
12. Chunks of 5,000:
    ```cypher
    UNWIND $batch AS row
    MERGE (i:Intersection {osmid: row.osmid})
    SET i += {latitude: row.latitude, longitude: row.longitude, street_count: row.street_count}
    ```

---

## Phase 5 — Batch Load Relationships — *depends on Phase 4*

13. Iterate `G.edges(keys=True, data=True)` → dicts with `u`, `v`, `osmid`, `name`, `highway`, `oneway`, `length`, `speed_kph`, `travel_time`, `lanes`
    - Edge `osmid` can be a list (consolidated ways) → take `osmid[0]`
    - `geometry` (Shapely LineString) **skipped** for now — can be added as WKT later
    - OSMNx already mirrors bidirectional streets as two directed edges — no manual duplication needed
14. Chunks of 5,000:
    ```cypher
    UNWIND $batch AS row
    MATCH (a:Intersection {osmid: row.u}), (b:Intersection {osmid: row.v})
    MERGE (a)-[r:ROAD_SEGMENT {osmid: row.osmid}]->(b)
    SET r += {name: row.name, highway: row.highway, oneway: row.oneway,
              length: row.length, speed_kph: row.speed_kph,
              travel_time: row.travel_time, lanes: row.lanes}
    ```

---

## Phase 6 — Shortest Path Queries (`src/queries.py`)

15. **Hop count** (built-in Cypher, no plugin required):
    ```cypher
    MATCH (a:Intersection {osmid: $start}), (b:Intersection {osmid: $end})
    MATCH path = shortestPath((a)-[*..200]->(b))
    RETURN path, length(path) AS hop_count
    ```

16. **Weighted Dijkstra** (GDS plugin):
    ```cypher
    -- Project a named graph
    CALL gds.graph.project(
      'road_net',
      'Intersection',
      'ROAD_SEGMENT',
      { relationshipProperties: ['travel_time', 'length'] }
    );

    -- Run Dijkstra (fastest route by travel time)
    CALL gds.shortestPath.dijkstra.stream('road_net', {
      sourceNode: $startNodeId,
      targetNode: $endNodeId,
      relationshipWeightProperty: 'travel_time'
    })
    YIELD index, totalCost, nodeIds, costs
    RETURN index, totalCost, nodeIds, costs;
    ```

---

## Project Structure

```
.
├── .env                        # Neo4j credentials (gitignored)
├── docker-compose.yml          # Neo4j 5 Community + GDS plugin
├── requirements.txt            # osmnx, neo4j, python-dotenv, shapely
├── data/
│   └── auckland_cbd.graphml    # Cached OSMNx graph (generated)
└── src/
    ├── download_network.py     # OSMNx download, enrich, cache
    ├── load_neo4j.py           # Schema setup + batched node/rel load
    └── queries.py              # Sample shortest-path Cypher queries
```

---

## Verification Checklist

1. `docker compose up -d` → open http://localhost:7474, confirm GDS shows in `:sysinfo`
2. `python src/download_network.py` → confirm `data/auckland_cbd.graphml` created; check printed node/edge counts
3. `python src/load_neo4j.py` → run `MATCH (i:Intersection) RETURN count(i)` — should match `len(G.nodes)`
4. `MATCH ()-[r:ROAD_SEGMENT]->() RETURN count(r)` — should match `len(G.edges)`
5. Run the hop-count `shortestPath` query in Neo4j Browser with two real `osmid` values
6. Project the GDS graph and run `gds.shortestPath.dijkstra.stream` — confirm `totalCost > 0`

---

## Decisions & Scope

- **Auckland CBD only** — avoids a multi-GB full-city graph; expand later by changing the place string
- **GDS via Docker env var** — `NEO4J_PLUGINS` auto-downloads the Community JAR; no manual placement
- **Edge geometry skipped** — keeps the model simple; add as WKT string later if spatial queries are needed
- **Enrichment in Python** — speed and travel_time computed before import; cheaper than post-load Neo4j compute
- **Credentials in `.env`** — loaded via `python-dotenv`, never hardcoded
