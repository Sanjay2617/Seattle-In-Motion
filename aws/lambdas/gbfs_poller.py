"""Lime Seattle GBFS poller Lambda, runs every 2 minutes. Env var: STREAM_NAME.
Uses the auto-discovery file so it works with GBFS v2 (free_bike_status)
or v3 (vehicle_status).

The vehicle feed is big (13k+ vehicles) and Lime's server is sometimes slow, so:
- ask for gzip to cut the download size
- allow a longer read timeout
- retry once with a short pause before giving up
- cache the feed URL between warm runs so most runs make one request, not two
If every attempt fails, the error is raised so the CloudWatch alarm still sees it."""
import gzip
import json
import time
import urllib.request

from kinesis_util import send

DISCOVERY = "https://data.lime.bike/api/partners/v2/gbfs/seattle/gbfs.json"
HEADERS = {"User-Agent": "seattle-in-motion (portfolio project)",
           "Accept-Encoding": "gzip"}
CHUNK = 500        # vehicles per Kinesis record, keeps each record well under 1 MB
TIMEOUT = 40       # seconds per request
ATTEMPTS = 2

_feed = {"url": None, "name": None}   # survives between warm invocations


def get_json(url):
    last_err = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                body = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
            return json.loads(body)
        except Exception as e:  # timeouts, connection resets, 5xx
            last_err = e
            print(f"attempt {attempt} failed for {url}: {e}")
            if attempt < ATTEMPTS:
                time.sleep(3)
    raise last_err


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
    if not _feed["url"]:
        _feed["url"], _feed["name"] = find_feed_url(get_json(DISCOVERY))
    try:
        feed = get_json(_feed["url"])
    except Exception:
        _feed["url"] = None   # rediscover next time in case the URL changed
        raise

    vehicles = feed["data"].get("bikes") or feed["data"].get("vehicles") or []
    chunks = []
    for i in range(0, len(vehicles), CHUNK):
        chunks.append({
            "feed": _feed["name"],
            "last_updated": feed.get("last_updated"),
            "chunk": i // CHUNK,
            "vehicles": vehicles[i:i + CHUNK],
        })
    n = send(chunks, "gbfs", key_field="chunk") if chunks else 0
    print(f"sent {len(vehicles)} vehicles in {n} records")
    return {"vehicles": len(vehicles)}
