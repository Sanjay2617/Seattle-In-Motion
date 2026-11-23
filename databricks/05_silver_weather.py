# Notebook: 05_silver_weather
from pyspark.sql import functions as F

CKPT = "/Volumes/seattle/raw/checkpoints/silver_weather"
SCHEMA = """payload STRUCT<station: STRING, observed_at: STRING, description: STRING,
  temp_c: DOUBLE, wind_kmh: DOUBLE, precip_last_hour_mm: DOUBLE, visibility_m: DOUBLE>"""

weather = (spark.readStream.table("seattle.bronze.events")
           .where("source = 'weather'")
           .select(F.from_json("raw_json", SCHEMA).alias("j"), "ingest_ts")
           .select("j.payload.*", "ingest_ts")
           .withColumn("observed_ts", F.to_timestamp("observed_at"))
           .withWatermark("observed_ts", "2 hours")
           # we poll every 10 min but NWS updates about hourly, so lots of repeats
           .dropDuplicatesWithinWatermark(["station", "observed_ts"]))

(weather.writeStream.format("delta")
 .option("checkpointLocation", CKPT)
 .trigger(availableNow=True)
 .toTable("seattle.silver.weather_obs")
 .awaitTermination())
