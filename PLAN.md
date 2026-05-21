# Plan: InfluxDB 3 + MCP Server for Traffic Time-Series

## TL;DR

Add InfluxDB 3 Core as a time-series database for the existing Kafka sensor data pipeline, then wire up the InfluxDB 3 MCP Server so an LLM agent (like Claude Desktop or Copilot) can query Wellington CBD traffic patterns in natural language. Restructure the repo to separate Neo4j and InfluxDB concerns.

**Use case**: The `kafka_producer.py` already emits `sensor-readings` (vehicle_count, avg_speed_kph per intersection) and `approach-sensors` (queue_length, arrival_rate, occupancy_pct per road segment) every 0.5s. InfluxDB stores this as high-resolution time-series, enabling queries like:
- "Show me intersections with average speed below 15 km/h during morning rush hour last week"
- "Compare queue lengths on Lambton Quay vs Willis Street this month"
- "Alert me when congestion at critical intersections exceeds 30 minutes"

---

## Phase 1: Project Restructuring

**Goal**: Separate concerns — Kafka (shared) stays at root, Neo4j pipeline in its own folder, InfluxDB pipeline in its own folder.

### New Structure
```
OpenStreetMap/
├── docker-compose.yml          (updated: adds InfluxDB 3 Core service)
├── requirements.txt            (updated: adds influxdb3-python)
├── start_pipeline.sh           (updated: option to start influx consumer too)
├── README.md
├── SHOWCASE.md
├── .env                        (shared env: Kafka, Neo4j, InfluxDB)
├── src/
│   ├── kafka_producer.py       (stays — shared producer for both pipelines)
│   ├── download_network.py     (stays — shared data prep)
│   ├── neo4j/
│   │   ├── __init__.py
│   │   ├── schema.py
│   │   ├── ingest_to_neo4j.py
│   │   ├── kafka_consumer.py
│   │   ├── text2cypher.py
│   │   ├── load_advisories.py
│   │   ├── graphrag_retriever.py
│   │   ├── gds_analytics.py
│   │   ├── traffic_history.py
│   │   └── traffic_light_controller.py
│   └── influxdb/
│       ├── __init__.py
│       ├── influx_consumer.py  (NEW — Kafka→InfluxDB sink)
│       └── context/
│           └── database-context.md  (MCP context doc for LLM)
├── influxdb-mcp/
│   └── mcp.json                (MCP server config for Claude Desktop / VS Code)
├── cache/
├── data/
└── logs/
```

### Steps
1. Create `src/neo4j/` directory, move Neo4j-specific files into it, add `__init__.py`
2. Create `src/influxdb/` directory with `__init__.py`
3. Update any relative imports in moved files
4. Update `start_pipeline.sh` to reference new paths

---

## Phase 2: InfluxDB 3 Core Docker Setup

**Goal**: Add InfluxDB 3 Core (open source) to `docker-compose.yml`.

### Steps
5. Add `influxdb3-core` service to `docker-compose.yml`:
   - Image: `quay.io/influxdb/influxdb3-core:latest`
   - Port: 8181 (HTTP API)
   - Volume: `./influxdb-data` for persistence
   - Environment: configure object store (local filesystem)
6. Add InfluxDB env vars to `.env`:
   - `INFLUX_DB_INSTANCE_URL=http://localhost:8181`
   - `INFLUX_DB_TOKEN=<generated-on-first-start>`
   - `INFLUX_DB_PRODUCT_TYPE=core`
7. Add `influxdb3-python` to `requirements.txt`

---

## Phase 3: Kafka→InfluxDB Consumer

**Goal**: New consumer that reads both Kafka topics and writes line protocol to InfluxDB.

### Steps
8. Create `src/influxdb/influx_consumer.py`:
   - Subscribe to `sensor-readings` and `approach-sensors` topics
   - Convert JSON messages to InfluxDB line protocol:
     - **Measurement `traffic_sensor`**: tags=`intersection_osmid`; fields=`vehicle_count`, `avg_speed_kph`; timestamp from `ts`
     - **Measurement `approach_sensor`**: tags=`from_osmid`, `to_osmid`; fields=`queue_length`, `arrival_rate`, `occupancy_pct`; timestamp from `ts`
   - Batch writes (every 100 points or 1 second, whichever first)
   - Use `influxdb3-python` client library
   - Target database: `traffic` (auto-created on first write in Core)

### Data Model (InfluxDB Line Protocol)
```
traffic_sensor,intersection_osmid=2584656974 vehicle_count=23i,avg_speed_kph=34.5 1716307200000000000
approach_sensor,from_osmid=2584656974,to_osmid=2584657001 queue_length=12i,arrival_rate=8.5,occupancy_pct=45.2 1716307200000000000
```

---

## Phase 4: MCP Server Setup

**Goal**: Deploy the official InfluxDB 3 MCP Server so an LLM can query traffic data conversationally.

### Steps
9. Create `influxdb-mcp/mcp.json` with MCP server configuration pointing to local InfluxDB Core:
   ```json
   {
     "mcpServers": {
       "influxdb": {
         "command": "npx",
         "args": ["-y", "@influxdata/influxdb3-mcp-server"],
         "env": {
           "INFLUX_DB_INSTANCE_URL": "http://localhost:8181/",
           "INFLUX_DB_TOKEN": "<YOUR_TOKEN>",
           "INFLUX_DB_PRODUCT_TYPE": "core"
         }
       }
     }
   }
   ```
10. Create `src/influxdb/context/database-context.md` — custom context file describing the traffic schema so the MCP server/LLM knows what data is available:
    - Document measurements (`traffic_sensor`, `approach_sensor`)
    - Document all tags and fields with descriptions and units
    - Document the Wellington CBD context (273 intersections, 539 road segments)
    - Include example natural language queries and their SQL equivalents
11. Optionally add the MCP config to VS Code's `.vscode/mcp.json` for integrated Copilot usage

---

## Phase 5: Verification & Documentation

### Steps
12. Start the full stack: `docker compose up -d` (Neo4j + Kafka + InfluxDB)
13. Run `kafka_producer.py` to generate data
14. Run `influx_consumer.py` — verify data appears in InfluxDB (SQL query via CLI or HTTP)
15. Test MCP server: configure in Claude Desktop or VS Code, ask natural language query
16. Update README.md with new Phase 11 (InfluxDB + MCP) section
17. Update SHOWCASE.md to add the time-series + conversational intelligence layer

---

## Decisions

| Decision | Rationale |
|----------|-----------|
| InfluxDB 3 Core (open source) | Simplest for local dev, no cloud account needed |
| npx-based MCP server | No Docker build needed, just `npx @influxdata/influxdb3-mcp-server` |
| Separate consumer (not modifying neo4j consumer) | Clean separation, both run independently against same Kafka topics |
| Tags: `intersection_osmid` / `from_osmid` + `to_osmid` | 273 unique values — fine cardinality for InfluxDB |
| Database name: `traffic` | Auto-created on first write in Core mode |
| Batch writes (100 pts or 1s) | Performance best practice for InfluxDB |

---

## Example MCP Queries (once operational)

| Natural Language | Expected SQL Generated |
|---|---|
| "Show me intersections with average speed below 15 km/h in the last hour" | `SELECT intersection_osmid, AVG(avg_speed_kph) as avg_speed FROM traffic_sensor WHERE time > now() - INTERVAL '1 hour' GROUP BY intersection_osmid HAVING AVG(avg_speed_kph) < 15` |
| "What's the busiest intersection right now?" | `SELECT intersection_osmid, MAX(vehicle_count) FROM traffic_sensor WHERE time > now() - INTERVAL '5 minutes' GROUP BY intersection_osmid ORDER BY MAX(vehicle_count) DESC LIMIT 1` |
| "Compare queue lengths between intersections 2584656974 and 2584657001 today" | `SELECT from_osmid, AVG(queue_length) FROM approach_sensor WHERE time > now() - INTERVAL '1 day' AND from_osmid IN ('2584656974','2584657001') GROUP BY from_osmid` |
