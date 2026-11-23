# Notebook: 03_silver_flights
from pyspark.sql import functions as F

CKPT = "/Volumes/seattle/raw/checkpoints/silver_flights"
SCHEMA = """payload STRUCT<icao24: STRING, callsign: STRING, origin_country: STRING,
  time_position: BIGINT, last_contact: BIGINT, longitude: DOUBLE, latitude: DOUBLE,
  baro_altitude: DOUBLE, on_ground: BOOLEAN, velocity: DOUBLE, true_track: DOUBLE,
  vertical_rate: DOUBLE, geo_altitude: DOUBLE, squawk: STRING, snapshot_time: BIGINT,
  registration: STRING, aircraft_type: STRING>"""

flights = (spark.readStream.table("seattle.bronze.events")
           .where("source = 'adsb'")
           .select(F.from_json("raw_json", SCHEMA).alias("j"), "ingest_ts")
           .select("j.payload.*", "ingest_ts")
           .withColumn("event_ts", F.timestamp_seconds(F.coalesce("time_position", "last_contact")))
           .withColumn("snapshot_ts", F.timestamp_seconds("snapshot_time"))
           .withColumnRenamed("latitude", "lat").withColumnRenamed("longitude", "lon")
           .where("lat IS NOT NULL AND lon IS NOT NULL")
           .withColumn("h3_r7", F.expr("h3_longlatash3(lon, lat, 7)"))
           .withWatermark("snapshot_ts", "30 minutes")
           .dropDuplicatesWithinWatermark(["icao24", "snapshot_ts"]))

(flights.writeStream.format("delta")
 .option("checkpointLocation", CKPT)
 .trigger(availableNow=True)
 .toTable("seattle.silver.aircraft_states")
 .awaitTermination())
