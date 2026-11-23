"""Weather poller Lambda (National Weather Service, SEA airport station), every 10 minutes.
Env var: STREAM_NAME. NWS needs a User-Agent with some contact info, no API key."""
import json
import urllib.request

from kinesis_util import send

URL = "https://api.weather.gov/stations/KSEA/observations/latest"
HEADERS = {"User-Agent": "seattle-in-motion (your-email@example.com)",
           "Accept": "application/geo+json"}


def val(props, key):
    v = props.get(key) or {}
    return v.get("value")


def lambda_handler(event, context):
    with urllib.request.urlopen(urllib.request.Request(URL, headers=HEADERS), timeout=15) as r:
        props = json.load(r)["properties"]
    row = {
        "station": "KSEA",
        "observed_at": props.get("timestamp"),
        "description": props.get("textDescription"),
        "temp_c": val(props, "temperature"),
        "wind_kmh": val(props, "windSpeed"),
        "precip_last_hour_mm": val(props, "precipitationLastHour"),
        "visibility_m": val(props, "visibility"),
    }
    send([row], "weather", key_field="station")
    return row
