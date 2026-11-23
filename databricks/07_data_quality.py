# Notebook: 07_data_quality. Last task in the job. Fails the job (and emails you)
# if any source has gone quiet, so you find out before an interviewer does.
from pyspark.sql import functions as F

MAX_AGE_MIN = {"ais": 45, "adsb": 30, "gbfs": 30, "weather": 120}

fresh = (spark.table("seattle.bronze.events")
         .groupBy("source")
         .agg(F.max("ingest_ts").alias("latest"))
         .withColumn("age_min",
                     (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("latest")) / 60)
         .withColumn("checked_at", F.current_timestamp()))

fresh.write.mode("append").saveAsTable("seattle.gold.dq_freshness")

rows = {r["source"]: r["age_min"] for r in fresh.collect()}
problems = [f"{s}: {round(rows.get(s, 1e9))} min old (limit {lim})"
            for s, lim in MAX_AGE_MIN.items() if rows.get(s, 1e9) > lim]
if problems:
    raise Exception("Stale sources -> " + "; ".join(problems))
print("All sources fresh:", rows)
