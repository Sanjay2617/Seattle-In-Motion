# Notebook: 01_bronze_ingest
# Picks up every new file in the landing volume exactly once (Auto Loader keeps
# track in the checkpoint) and stores each JSON line untouched.
from pyspark.sql import functions as F

LANDING = "/Volumes/seattle/raw/landing/"
CKPT = "/Volumes/seattle/raw/checkpoints/bronze_events"

raw = (spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "text")   # one row per JSON line; gzip is read automatically
       .load(LANDING))

bronze = raw.select(
    F.col("value").alias("raw_json"),
    F.get_json_object("value", "$.source").alias("source"),
    F.timestamp_seconds(F.get_json_object("value", "$.ingest_ts").cast("double")).alias("ingest_ts"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("bronze_loaded_at"),
)

(bronze.writeStream
 .format("delta")
 .option("checkpointLocation", CKPT)
 .trigger(availableNow=True)   # process everything new, then stop (cheap, serverless-friendly)
 .toTable("seattle.bronze.events")
 .awaitTermination())
