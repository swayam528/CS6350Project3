"""
Kibana Setup Script
===================
Automates creation of:
  1. An Elasticsearch index mapping for `ner-entity-counts`
  2. A Kibana index pattern for the same index
  3. A horizontal bar-chart visualisation: "Top 10 Named Entities by Count"
  4. A dashboard that embeds the visualisation

Run this AFTER:
  • docker-compose is up
  • The PySpark NER job has produced at least one trigger (i.e., `ner-entity-counts`
    already has documents in Elasticsearch)

Usage
-----
    python kibana_setup.py [kibana_url] [es_url]

    kibana_url  Default: http://localhost:5601
    es_url      Default: http://localhost:9200
"""

import json
import sys
import time

import requests

KIBANA_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5601"
ES_URL     = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9200"

KIBANA_HEADERS = {
    "kbn-xsrf":     "true",
    "Content-Type": "application/json",
}
ES_HEADERS = {"Content-Type": "application/json"}

INDEX_NAME    = "ner-entity-counts"
VIZ_TITLE     = "Top 10 Named Entities by Count"
DASH_TITLE    = "NER Entity Counts Dashboard"


# ── Helpers ────────────────────────────────────────────────────────────────────

def wait_for_service(url: str, name: str, retries: int = 60, delay: int = 5) -> None:
    """Poll `url` until it responds with HTTP 200 (or non-50x)."""
    print(f"[setup] Waiting for {name} at {url} …", end="", flush=True)
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code < 500:
                print(" ready.")
                return
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.Timeout):
            pass
        print(".", end="", flush=True)
        time.sleep(delay)
    raise RuntimeError(f"{name} did not become ready after {retries * delay}s.")


def kibana_post(path: str, body: dict) -> requests.Response:
    r = requests.post(f"{KIBANA_URL}{path}", headers=KIBANA_HEADERS, json=body, timeout=30)
    return r


def kibana_get(path: str) -> requests.Response:
    r = requests.get(f"{KIBANA_URL}{path}", headers=KIBANA_HEADERS, timeout=30)
    return r


# ── Step 1: Ensure Elasticsearch index mapping ────────────────────────────────

def create_es_mapping() -> None:
    """Create the index with explicit field mappings if it doesn't exist yet."""
    url     = f"{ES_URL}/{INDEX_NAME}"
    mapping = {
        "mappings": {
            "properties": {
                "entity":      {"type": "keyword"},
                "label":       {"type": "keyword"},
                "count":       {"type": "long"},
                "ingested_at": {"type": "date"},
            }
        }
    }
    r = requests.put(url, headers=ES_HEADERS, json=mapping, timeout=30)
    if r.status_code in (200, 400):          # 400 = index already exists, fine
        msg = "created" if r.status_code == 200 else "already exists"
        print(f"[setup] ES index '{INDEX_NAME}' {msg}.")
    else:
        print(f"[setup] Warning: unexpected ES response {r.status_code}: {r.text}")


# ── Step 2: Create Kibana index pattern ───────────────────────────────────────

def create_index_pattern() -> str:
    """Create index pattern and return its saved-object ID."""
    # Check whether it already exists
    r = kibana_get(f"/api/saved_objects/_find?type=index-pattern&search_fields=title&search={INDEX_NAME}")
    if r.ok:
        hits = r.json().get("saved_objects", [])
        for hit in hits:
            if hit["attributes"].get("title") == INDEX_NAME:
                pid = hit["id"]
                print(f"[setup] Index pattern '{INDEX_NAME}' already exists (id={pid}).")
                return pid

    body = {
        "attributes": {
            "title": INDEX_NAME,
        }
    }
    r = kibana_post("/api/saved_objects/index-pattern", body)
    if not r.ok:
        raise RuntimeError(f"Failed to create index pattern: {r.status_code} {r.text}")
    pid = r.json()["id"]
    print(f"[setup] Created index pattern '{INDEX_NAME}' (id={pid}).")
    return pid


# ── Step 3: Create horizontal bar-chart visualisation ─────────────────────────

def create_visualization(index_pattern_id: str) -> str:
    """
    Create a horizontal bar chart: entity (y-axis, top 10) vs. count (x-axis).
    Uses a `max` metric on the `count` field so repeated documents don't inflate
    the displayed value beyond the latest running count from PySpark.
    """
    vis_state = {
        "title": VIZ_TITLE,
        "type":  "horizontal_bar",
        "params": {
            "type":       "horizontal_bar",
            "addTooltip": True,
            "addLegend":  True,
            "legendPosition": "right",
            "times": [],
            "addTimeMarker": False,
            "maxBarWidth":  0.5,
            "categoryAxes": [
                {
                    "id":       "CategoryAxis-1",
                    "type":     "category",
                    "position": "left",
                    "show":     True,
                    "scale":    {"type": "linear"},
                    "labels":   {"show": True, "filter": False, "truncate": 400, "rotate": 0},
                    "title":    {"text": "Entity"},
                }
            ],
            "valueAxes": [
                {
                    "id":       "ValueAxis-1",
                    "name":     "BottomAxis-1",
                    "type":     "value",
                    "position": "bottom",
                    "show":     True,
                    "scale":    {"type": "linear", "mode": "normal"},
                    "labels":   {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                    "title":    {"text": "Running Count"},
                }
            ],
            "seriesParams": [
                {
                    "show":      True,
                    "type":      "histogram",
                    "mode":      "normal",
                    "data":      {"label": "Count", "id": "1"},
                    "valueAxis": "ValueAxis-1",
                }
            ],
        },
        "aggs": [
            {
                "id":      "1",
                "enabled": True,
                "type":    "max",                   # latest running count per entity
                "schema":  "metric",
                "params":  {"field": "count"},
            },
            {
                "id":      "2",
                "enabled": True,
                "type":    "terms",
                "schema":  "segment",
                "params": {
                    "field":              "entity.keyword",
                    "orderBy":            "1",
                    "order":              "desc",
                    "size":               10,
                    "otherBucket":        False,
                    "missingBucket":      False,
                },
            },
        ],
    }

    body = {
        "attributes": {
            "title":       VIZ_TITLE,
            "visState":    json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": "Running count of named entities from NER Spark streaming job",
            "version":     1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index":  index_pattern_id,
                    "query":  {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        }
    }

    r = kibana_post("/api/saved_objects/visualization", body)
    if not r.ok:
        raise RuntimeError(f"Failed to create visualisation: {r.status_code} {r.text}")
    vid = r.json()["id"]
    print(f"[setup] Created visualisation '{VIZ_TITLE}' (id={vid}).")
    return vid


# ── Step 4: Create dashboard ──────────────────────────────────────────────────

def create_dashboard(viz_id: str) -> str:
    panels = [
        {
            "embeddableConfig": {},
            "gridData":         {"x": 0, "y": 0, "w": 48, "h": 40, "i": "1"},
            "id":               viz_id,
            "panelIndex":       "1",
            "type":             "visualization",
            "version":          "7.17.20",
        }
    ]
    body = {
        "attributes": {
            "title":            DASH_TITLE,
            "hits":             0,
            "description":      "Live top-10 named entities from the NER Spark streaming pipeline",
            "panelsJSON":       json.dumps(panels),
            "optionsJSON":      json.dumps({"useMargins": True, "hidePanelTitles": False}),
            "version":          1,
            "timeRestore":      False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        }
    }
    r = kibana_post("/api/saved_objects/dashboard", body)
    if not r.ok:
        raise RuntimeError(f"Failed to create dashboard: {r.status_code} {r.text}")
    did = r.json()["id"]
    print(f"[setup] Created dashboard '{DASH_TITLE}' (id={did}).")
    return did


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[setup] Kibana : {KIBANA_URL}")
    print(f"[setup] ES     : {ES_URL}")

    wait_for_service(f"{ES_URL}/_cluster/health",    "Elasticsearch")
    wait_for_service(f"{KIBANA_URL}/api/status",     "Kibana")

    # Extra wait: Kibana sometimes needs a moment after reporting "available"
    time.sleep(3)

    create_es_mapping()
    pattern_id = create_index_pattern()
    viz_id     = create_visualization(pattern_id)
    dash_id    = create_dashboard(viz_id)

    print()
    print("=" * 65)
    print("  Setup complete!")
    print(f"  Open Kibana: {KIBANA_URL}")
    print(f"  Dashboard  : {KIBANA_URL}/app/dashboards#/view/{dash_id}")
    print()
    print("  Tip: enable auto-refresh (e.g., every 30 s) in the dashboard")
    print("       to watch entity counts update in real time.")
    print("=" * 65)


if __name__ == "__main__":
    main()
