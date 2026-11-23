# Notebook: 04_silver_bikes
from pyspark.sql import functions as F

CKPT = "/Volumes/seattle/raw/checkpoints/silver_bikes"
SCHEMA = """payload STRUCT<feed: STRING, last_updated: BIGINT, chunk: INT,
  vehicles: ARRAY<STRUCT<bike_id: STRING, vehicle_id: STRING, lat: DOUBLE, lon: DOUBLE,
                         is_reserved: BOOLEAN, is_disabled: BOOLEAN,
                         vehicle_type_id: STRING>>>"""

bikes = (spark.readStream.table("seattle.bronze.events")
         .where("source = 'gbfs'")
         .select(F.from_json("raw_json", SCHEMA).alias("j"), "ingest_ts")
         .select(F.timestamp_seconds("j.payload.last_updated").alias("snapshot_ts"),
                 F.explode("j.payload.vehicles").alias("v"), "ingest_ts")
         .select("snapshot_ts",
                 F.coalesce("v.vehicle_id", "v.bike_id").alias("vehicle_id"),  # v3 vs v2 name
                 "v.lat", "v.lon", "v.is_reserved", "v.is_disabled",
                 "v.vehicle_type_id", "ingest_ts")
         .withColumn("h3_r8", F.expr("h3_longlatash3(lon, lat, 8)"))
         .withWatermark("snapshot_ts", "30 minutes")
         .dropDuplicatesWithinWatermark(["vehicle_id", "snapshot_ts"]))

(bikes.writeStream.format("delta")
 .option("checkpointLocation", CKPT)
 .trigger(availableNow=True)
 .toTable("seattle.silver.vehicle_snapshots")
 .awaitTermination())
