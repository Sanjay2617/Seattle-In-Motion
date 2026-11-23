"""Lime Seattle GBFS poller Lambda, runs every 2 minutes. Env var: STREAM_NAME.
Uses the auto-discovery file so it works with GBFS v2 (free_bike_status)
or v3 (vehicle_status)."""
import json
import urllib.request

from kinesis_util import send

DISCOVERY = "https://data.lime.bike/api/partners/v2/gbfs/seattle/gbfs.json"
CHUNK = 500  # vehicles per Kinesis record, keeps each record well under 1 MB


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "seattle-in-motion (portfolio project)"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def find_feed_url(discovery):
    data = discovery["data"]
    # v2 nests feeds under a language key like "en"; v3 puts them directly under data
    feeds = data["feeds"] if "feeds" in data else next(iter(data.values()))["feeds"]
    for name in ("vehicle_status", "free_bike_status"):
        for f in feeds:
            if f["name"] == name:
                return f["url"], name
    raise RuntimeError("no vehicle feed found")


def lambda_handler(event, context):
    url, feed_name = find_feed_url(get_json(DISCOVERY))
    feed = get_json(url)
    vehicles = feed["data"].get("bikes") or feed["data"].get("vehicles") or []
    chunks = []
    for i in range(0, len(vehicles), CHUNK):
        chunks.append({
            "feed": feed_name,
            "last_updated": feed.get("last_updated"),
            "chunk": i // CHUNK,
            "vehicles": vehicles[i:i + CHUNK],
        })
    n = send(chunks, "gbfs", key_field="chunk") if chunks else 0
    print(f"sent {len(vehicles)} vehicles in {n} records")
    return {"vehicles": len(vehicles)}
