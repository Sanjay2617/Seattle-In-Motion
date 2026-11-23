"""Aircraft poller Lambda using adsb.lol (free, open, no API key). Runs every 1 minute.
Replaces the OpenSky poller, since OpenSky blocks requests coming from AWS IPs.
Output fields use the same names as the OpenSky version (meters, m/s) so the
silver notebook only needs its source filter changed to 'adsb'.
Env var: STREAM_NAME."""
import json
import time
import urllib.request

from kinesis_util import send

# Center on Seattle, 40 nautical mile radius (covers SEA, Boeing Field, Paine Field)
URL = "https://api.adsb.lol/v2/lat/47.55/lon/-122.30/dist/40"
HEADERS = {"User-Agent": "seattle-in-motion (portfolio project)"}
FT_TO_M, KT_TO_MS, FPM_TO_MS = 0.3048, 0.514444, 0.00508


def num(v, factor=1.0):
    return round(v * factor, 2) if isinstance(v, (int, float)) else None


def lambda_handler(event, context):
    with urllib.request.urlopen(urllib.request.Request(URL, headers=HEADERS), timeout=20) as r:
        body = json.load(r)
    snap = int(body.get("now", time.time() * 1000) / 1000)  # adsb.lol gives ms

    rows = []
    for a in body.get("ac") or []:
        if a.get("lat") is None or a.get("lon") is None:
            continue
        alt = a.get("alt_baro")
        rows.append({
            "icao24": a.get("hex"),
            "callsign": (a.get("flight") or "").strip(),
            "registration": a.get("r"),
            "aircraft_type": a.get("t"),
            "origin_country": None,
            "time_position": snap - int(a["seen_pos"]) if isinstance(a.get("seen_pos"), (int, float)) else None,
            "last_contact": snap - int(a["seen"]) if isinstance(a.get("seen"), (int, float)) else snap,
            "latitude": a["lat"],
            "longitude": a["lon"],
            "baro_altitude": None if alt == "ground" else num(alt, FT_TO_M),
            "on_ground": alt == "ground",
            "velocity": num(a.get("gs"), KT_TO_MS),
            "true_track": num(a.get("track")),
            "vertical_rate": num(a.get("baro_rate"), FPM_TO_MS),
            "geo_altitude": num(a.get("alt_geom"), FT_TO_M),
            "squawk": a.get("squawk"),
            "snapshot_time": snap,
        })
    n = send(rows, "adsb", key_field="icao24") if rows else 0
    print(f"sent {n} aircraft")
    return {"sent": n}
