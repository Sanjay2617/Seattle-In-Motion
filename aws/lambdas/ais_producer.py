"""AIS producer Lambda. Runs ~14.5 minutes, then exits.
EventBridge Scheduler starts a new run every 15 minutes, so the gap is a few seconds.
Needs: websockets (packaged in the zip), kinesis_util.py, env vars STREAM_NAME, AIS_API_KEY."""
import asyncio
import json
import os
import time

import websockets

from kinesis_util import send

API_KEY = os.environ["AIS_API_KEY"]
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "870"))
# AISStream wants [[lat, lon], [lat, lon]] corners. This box covers Elliott Bay,
# the Seattle ferry lanes and a bit of Puget Sound around Bainbridge.
BBOX = [[[47.50, -122.55], [47.72, -122.30]]]
SENT = {"n": 0}  # running count, survives reconnects


async def stream(deadline):
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    buf, last_flush = [], time.time()
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream",
                                      ping_interval=20) as ws:
            # AISStream closes the socket if this isn't sent within 3 seconds
            await ws.send(json.dumps(sub))
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    msg = json.loads(raw)
                    msg["mmsi"] = msg.get("MetaData", {}).get("MMSI")
                    buf.append(msg)
                except asyncio.TimeoutError:
                    pass  # quiet water, just check the clock and flush
                if len(buf) >= 200 or (buf and time.time() - last_flush >= 5):
                    SENT["n"] += send(buf, "ais", key_field="mmsi")
                    buf, last_flush = [], time.time()
    finally:
        # runs on a clean finish AND when the socket drops, so nothing buffered is lost
        if buf:
            SENT["n"] += send(buf, "ais", key_field="mmsi")
            print(f"flushed {len(buf)} buffered messages")


async def main():
    deadline = time.time() + RUN_SECONDS
    SENT["n"], backoff = 0, 1
    while time.time() < deadline - 10:
        try:
            await stream(deadline)
        except Exception as e:  # dropped socket, DNS blip, etc.
            print(f"reconnecting after error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return SENT["n"]


def lambda_handler(event, context):
    total = asyncio.run(main())
    print(f"sent {total} AIS messages")
    return {"sent": total}
