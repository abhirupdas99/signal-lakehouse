# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: raw, append-only ingestion with Auto Loader
# MAGIC
# MAGIC Every file that lands in the volume is ingested **exactly once** into a bronze Delta table.
# MAGIC Bronze keeps everything, including broken rows; filtering happens in silver so nothing is
# MAGIC ever silently dropped.
# MAGIC
# MAGIC **Why streaming at ~1M rows a month?** Not for speed. The checkpoint remembers which files
# MAGIC were already processed, so reruns only pick up new files. `availableNow` processes the
# MAGIC backlog and stops, so it costs the same as a batch job. On serverless it's also the only
# MAGIC trigger allowed.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{CATALOG}/signal_landing/raw"
CHECKPOINTS = f"/Volumes/{CATALOG}/signal_landing/checkpoints"
BRONZE = f"{CATALOG}.signal_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.signal_landing.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source schemas = the data contract
# MAGIC Declaring schemas up front (instead of inferring) means a producer can't silently change
# MAGIC a type on us. `_corrupt_record` catches JSON lines that can't be parsed; `_rescued_data`
# MAGIC catches CSV values that don't fit the declared type.

# COMMAND ----------

def s(*names, t=StringType()):
    return [StructField(n, t) for n in names]

impressions_schema = StructType(
    s("impression_id", "event_ts", "campaign_id", "station_id", "content_type", "device", "ip", "maid")
    + s("lat", "lon", t=DoubleType())
    + s("_corrupt_record")
)
conversions_schema = StructType(
    s("conversion_id", "advertiser_id", "conversion_type", "ip", "maid", "event_ts")
)
campaigns_cdc_schema = StructType(
    s("campaign_id", "advertiser_id", "campaign_name")
    + s("budget_usd", t=IntegerType())
    + s("status", "op", "updated_at")
)
stations_schema = StructType(
    s("station_id", "station_name", "content_type", "genre", "market")
)

# COMMAND ----------

def rows_processed(query):
    """Rows read in this run, summed over all micro-batches (works on classic and serverless)."""
    total = 0
    for p in query.recentProgress:
        total += p["numInputRows"] if isinstance(p, dict) else p.numInputRows
    return total


def ingest(folder, fmt, schema, options):
    """Auto Loader: landing folder -> bronze table, with lineage columns, exactly once."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .options(**options)
        .schema(schema)
        .load(f"{LANDING}/{folder}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    query = (
        stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_{folder}")
        .trigger(availableNow=True)
        .toTable(f"{BRONZE}.{folder}")
    )
    query.awaitTermination()
    print(f"{folder:<14} -> {BRONZE}.{folder}: {rows_processed(query):,} new rows this run")

# COMMAND ----------

ingest("impressions", "json", impressions_schema,
       {"columnNameOfCorruptRecord": "_corrupt_record"})

csv_options = {"header": "true", "rescuedDataColumn": "_rescued_data"}
ingest("conversions", "csv", conversions_schema, csv_options)
ingest("campaigns_cdc", "csv", campaigns_cdc_schema, csv_options)
ingest("stations", "csv", stations_schema, csv_options)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check your work
# MAGIC Expect roughly 33.8k impression lines per day generated, a few hundred corrupt lines in
# MAGIC total, and one source file per day. Rerun this notebook: every table should report
# MAGIC **0 new rows**. That's the checkpoint doing its job.

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'impressions' AS table_name, count(*) AS rows, count(DISTINCT _source_file) AS files,
         count_if(_corrupt_record IS NOT NULL) AS corrupt_rows
  FROM {BRONZE}.impressions
  UNION ALL
  SELECT 'conversions', count(*), count(DISTINCT _source_file), count_if(_rescued_data IS NOT NULL)
  FROM {BRONZE}.conversions
  UNION ALL
  SELECT 'campaigns_cdc', count(*), count(DISTINCT _source_file), count_if(_rescued_data IS NOT NULL)
  FROM {BRONZE}.campaigns_cdc
  UNION ALL
  SELECT 'stations', count(*), count(DISTINCT _source_file), count_if(_rescued_data IS NOT NULL)
  FROM {BRONZE}.stations
"""))
