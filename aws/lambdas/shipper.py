"""Shipper Lambda: fires on every new Firehose file in S3 and uploads it to a
Databricks Unity Catalog volume using the Files API. This is the bridge that makes
Databricks Free Edition work, since Free Edition can't read your S3 bucket directly.
Env vars: DATABRICKS_HOST (like https://dbc-xxxx.cloud.databricks.com),
          VOLUME_PATH (like /Volumes/seattle/raw/landing),
          PAT_PARAM (SSM SecureString name holding your Databricks token)."""
import os
import urllib.parse
import urllib.request

import boto3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
VOLUME = os.environ["VOLUME_PATH"].rstrip("/")
_pat = None


def token():
    global _pat
    if _pat is None:
        _pat = ssm.get_parameter(Name=os.environ["PAT_PARAM"],
                                 WithDecryption=True)["Parameter"]["Value"]
    return _pat


def lambda_handler(event, context):
    for rec in event["Records"]:
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()

        # keep the yyyy/MM/dd/HH folders so the volume mirrors S3
        rel = key.split("raw/", 1)[-1]
        target = f"{VOLUME}/{rel}"
        url = f"{HOST}/api/2.0/fs/files{urllib.parse.quote(target)}?overwrite=true"
        req = urllib.request.Request(url, data=body, method="PUT", headers={
            "Authorization": f"Bearer {token()}",
            "Content-Type": "application/octet-stream",
        })
        with urllib.request.urlopen(req, timeout=60) as r:
            print(f"uploaded s3://{bucket}/{key} -> {target} ({len(body)} bytes, HTTP {r.status})")
    # Raising on failure (urlopen does that for 4xx/5xx) makes Lambda retry the
    # S3 event automatically, and overwrite=true makes the retry safe.
