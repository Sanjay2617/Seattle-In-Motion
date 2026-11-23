"""Shared helper: send a list of dicts to Kinesis as newline-delimited JSON.
Put this file in every producer Lambda zip."""
import json
import os
import time

import boto3

STREAM = os.environ["STREAM_NAME"]
kinesis = boto3.client("kinesis")


def send(records, source, key_field=None):
    """records: list of dicts. source: 'ais' | 'opensky' | 'gbfs' | 'weather'."""
    now = time.time()
    entries = []
    for r in records:
        body = {"source": source, "ingest_ts": now, "payload": r}
        key = str(r.get(key_field, "na")) if key_field else source
        entries.append({
            # The trailing newline matters: Firehose glues records together,
            # so the newline turns the S3 file into proper JSON lines.
            "Data": (json.dumps(body) + "\n").encode("utf-8"),
            "PartitionKey": f"{source}-{key}",
        })

    # PutRecords takes at most 500 records per call
    for i in range(0, len(entries), 500):
        batch = entries[i:i + 500]
        for attempt in range(5):
            resp = kinesis.put_records(StreamName=STREAM, Records=batch)
            if resp["FailedRecordCount"] == 0:
                break
            # keep only the records that failed (usually throttling) and retry
            batch = [e for e, res in zip(batch, resp["Records"]) if "ErrorCode" in res]
            time.sleep(0.2 * (2 ** attempt))
        else:
            print(f"WARNING: gave up on {len(batch)} records")
    return len(entries)
