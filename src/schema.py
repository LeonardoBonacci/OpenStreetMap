"""
Neo4j schema for the Wellington CBD road network.

Labels
------
:Intersection
    osmid        INTEGER   -- OSM node ID (unique key)
    lat          FLOAT     -- WGS-84 latitude  (y)
    lon          FLOAT     -- WGS-84 longitude (x)
    street_count INTEGER   -- number of streets meeting at this intersection

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

Constraints & indexes
---------------------
UNIQUE  Intersection.osmid          (also creates a b-tree lookup index)
RANGE   Intersection(lat, lon)      (spatial-style bbox queries)
RANGE   ROAD(highway)               (filter by road class)
TEXT    ROAD(name)                  (substring / full-text search)
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
]


def apply(session):
    """Apply all schema statements to an open Neo4j session."""
    for stmt in SCHEMA_STATEMENTS:
        session.run(stmt)
    print(f"Schema applied ({len(SCHEMA_STATEMENTS)} statements).")
