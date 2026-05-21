"""
Phase 2: Download and enrich the Auckland CBD drivable road network via OSMNx.
Caches the result to data/auckland_cbd.graphml so re-runs skip the download.
"""

from pathlib import Path
import osmnx as ox

# Use a reliable Overpass API mirror
ox.settings.overpass_url = "https://overpass.kumi.systems/api/interpreter"

# Bounding box for Wellington CBD (compact ~1km x 1km)
BBOX = (-41.276, 174.785, -41.295, 174.770)  # (north, east, south, west)
CACHE_PATH = Path(__file__).parent.parent / "data" / "wellington_cbd.graphml"


def download_and_enrich():
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

    if CACHE_PATH.exists():
        print(f"Loading cached graph from {CACHE_PATH}")
        G = ox.load_graphml(CACHE_PATH)
    else:
        north, east, south, west = BBOX
        print(f"Downloading road network for Wellington CBD bbox")
        G = ox.graph_from_bbox((west, south, east, north), network_type="drive")
        G = ox.add_edge_speeds(G, fallback=50)
        G = ox.add_edge_travel_times(G)
        ox.save_graphml(G, CACHE_PATH)
        print(f"Saved graph to {CACHE_PATH}")

    print(f"Nodes : {len(G.nodes):,}")
    print(f"Edges : {len(G.edges):,}")
    return G


if __name__ == "__main__":
    download_and_enrich()
