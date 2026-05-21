"""
Neo4j schema for the Wellington CBD road network.

Labels
------
:Intersection
    osmid        INTEGER   -- OSM node ID (unique key)
    lat          FLOAT     -- WGS-84 latitude  (y)
    lon          FLOAT     -- WGS-84 longitude (x)
    street_count INTEGER   -- number of streets meeting at this intersection

:Advisory  (Phase 6)
    id           STRING    -- unique advisory identifier (e.g. 'adv-001')
    title        STRING    -- short headline
    text         STRING    -- full advisory text
    published    STRING    -- ISO date string
    embedding    LIST<FLOAT> -- nomic-embed-text vector (768 dims)

Relationship types
------------------
:ROAD  (:Intersection)-[:ROAD]->(:Intersection)
    osmid        INTEGER   -- OSM way ID
    name         STRING    -- street name
    highway      STRING    -- OSM highway tag  (e.g. 'residential', 'primary')
    maxspeed     STRING    -- posted speed limit as OSM string (e.g. '50')
    length       FLOAT     -- segment length in metres
    speed_kph    FLOAT     -- inferred/posted speed in km/h
    travel_time  FLOAT     -- estimated travel time in seconds
    oneway       BOOLEAN   -- true if travel is one-directional
    reversed     BOOLEAN   -- true if edge was reversed during graph simplification

:AFFECTS_ROAD  (:Advisory)-[:AFFECTS_ROAD]->(:Intersection)  (Phase 6)

:HAD_READING  (:Intersection)-[:HAD_READING]->(:TrafficSnapshot)  (Phase 9)

:TrafficSnapshot  (Phase 9)
    speed        FLOAT     -- recorded speed in km/h
    flow         INTEGER   -- vehicle count at time of reading
    recorded_at  DATETIME  -- Neo4j native datetime of the reading

:TrafficLight  (Phase 10)
    osmid        INTEGER   -- references parent Intersection (unique)
    cycle_time   INTEGER   -- total cycle length in seconds (e.g. 90)
    current_phase STRING   -- active phase label (e.g. 'NS_GREEN', 'EW_GREEN')
    phase_updated_at STRING -- ISO-8601 timestamp of last phase transition
    green_ns     FLOAT     -- fraction of cycle allocated to north-south (0–1)
    green_ew     FLOAT     -- fraction of cycle allocated to east-west (0–1)

:HAS_LIGHT  (:Intersection)-[:HAS_LIGHT]->(:TrafficLight)  (Phase 10)

Additional :ROAD properties (Phase 10 — approach sensors):
    sensor_queue       INTEGER -- vehicles queued approaching destination intersection
    sensor_arrival_rate FLOAT  -- vehicles/min arriving at destination
    sensor_occupancy   FLOAT   -- inductive loop occupancy % (0–100)
    sensor_ts          STRING  -- ISO-8601 timestamp of last sensor reading

Constraints & indexes
---------------------
UNIQUE  Intersection.osmid          (also creates a b-tree lookup index)
RANGE   Intersection(lat, lon)      (spatial-style bbox queries)
RANGE   ROAD(highway)               (filter by road class)
TEXT    ROAD(name)                  (substring / full-text search)
UNIQUE  Advisory.id                 (Phase 6)
VECTOR  Advisory(embedding)         (Phase 6 — 768 dims, cosine, nomic-embed-text)
RANGE   TrafficSnapshot(recorded_at) (Phase 9 — temporal queries)
UNIQUE  TrafficLight.osmid           (Phase 10)
RANGE   ROAD(sensor_queue)           (Phase 10 — approach sensor queries)
"""

SCHEMA_STATEMENTS = [
    # ---------- constraints ----------
    """CREATE CONSTRAINT intersection_osmid IF NOT EXISTS
       FOR (n:Intersection) REQUIRE n.osmid IS UNIQUE""",

    # ---------- indexes ----------
    """CREATE INDEX intersection_location IF NOT EXISTS
       FOR (n:Intersection) ON (n.lat, n.lon)""",

    """CREATE INDEX road_highway IF NOT EXISTS
       FOR ()-[r:ROAD]-() ON (r.highway)""",

    """CREATE TEXT INDEX road_name IF NOT EXISTS
       FOR ()-[r:ROAD]-() ON (r.name)""",

    # ---------- Phase 6: Advisory nodes ----------
    """CREATE CONSTRAINT advisory_id IF NOT EXISTS
       FOR (a:Advisory) REQUIRE a.id IS UNIQUE""",

    # 768 dimensions matches nomic-embed-text (default OLLAMA_EMBED_MODEL).
    # If you switch embedding models, drop this index and recreate with the
    # correct dimension count before re-running load_advisories.py.
    """CREATE VECTOR INDEX advisory_text IF NOT EXISTS
       FOR (a:Advisory) ON (a.embedding)
       OPTIONS {indexConfig: {`vector.dimensions`: 768,
                              `vector.similarity_function`: 'cosine'}}""",

    # ---------- Phase 9: Temporal traffic snapshots ----------
    """CREATE INDEX snapshot_recorded_at IF NOT EXISTS
       FOR (s:TrafficSnapshot) ON (s.recorded_at)""",

    # ---------- Phase 10: Traffic light control ----------
    """CREATE CONSTRAINT traffic_light_osmid IF NOT EXISTS
       FOR (t:TrafficLight) REQUIRE t.osmid IS UNIQUE""",

    """CREATE INDEX road_sensor_queue IF NOT EXISTS
       FOR ()-[r:ROAD]-() ON (r.sensor_queue)""",
]


def apply(session):
    """Apply all schema statements to an open Neo4j session."""
    for stmt in SCHEMA_STATEMENTS:
        session.run(stmt)
    print(f"Schema applied ({len(SCHEMA_STATEMENTS)} statements).")
