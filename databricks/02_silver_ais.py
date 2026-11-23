# Notebook: 02_silver_ais
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")  # AIS timestamps are UTC

CKPT = "/Volumes/seattle/raw/checkpoints/silver_ais"
SCHEMA = """payload STRUCT<
  MessageType: STRING,
  MetaData: STRUCT<MMSI: BIGINT, ShipName: STRING, latitude: DOUBLE,
                   longitude: DOUBLE, time_utc: STRING>,
  Message: STRUCT<PositionReport: STRUCT<Sog: DOUBLE, Cog: DOUBLE,
                  TrueHeading: INT, NavigationalStatus: INT>>>"""

ais = (spark.readStream.table("seattle.bronze.events")
       .where("source = 'ais'")
       .select(F.from_json("raw_json", SCHEMA).alias("j"), "ingest_ts")
       .where("j.payload.MessageType = 'PositionReport'")
       .select(
           F.col("j.payload.MetaData.MMSI").alias("mmsi"),
           F.trim("j.payload.MetaData.ShipName").alias("ship_name"),
           F.col("j.payload.MetaData.latitude").alias("lat"),
           F.col("j.payload.MetaData.longitude").alias("lon"),
           # time_utc looks like "2026-11-23 19:05:12.123456789 +0000 UTC"
           F.to_timestamp(F.substring("j.payload.MetaData.time_utc", 1, 19),
                          "yyyy-MM-dd HH:mm:ss").alias("event_ts"),
           # AIS uses 102.3 for "speed unknown" and 360 for "course unknown", so make them null
           F.when(F.col("j.payload.Message.PositionReport.Sog") < 102.2,
                  F.col("j.payload.Message.PositionReport.Sog")).alias("speed_knots"),
           F.when(F.col("j.payload.Message.PositionReport.Cog") < 360,
                  F.col("j.payload.Message.PositionReport.Cog")).alias("course"),
           F.col("j.payload.Message.PositionReport.NavigationalStatus").alias("nav_status"),
           "ingest_ts")
       .where("lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND mmsi IS NOT NULL")
       .withColumn("h3_r8", F.expr("h3_longlatash3(lon, lat, 8)"))
       # same ship + same timestamp = duplicate (AIS stations often hear one ping twice)
       .withWatermark("event_ts", "30 minutes")
       .dropDuplicatesWithinWatermark(["mmsi", "event_ts"]))

(ais.writeStream.format("delta")
 .option("checkpointLocation", CKPT)
 .trigger(availableNow=True)
 .toTable("seattle.silver.ais_positions")
 .awaitTermination())
