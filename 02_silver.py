# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: validate, quarantine, dedupe, protect identifiers, keep history
# MAGIC
# MAGIC | Output table | What it holds |
# MAGIC | --- | --- |
# MAGIC | `signal_silver.impressions` | Valid, deduplicated impressions. IP and mobile ID hashed, geo rounded |
# MAGIC | `signal_silver.quarantine_impressions` | Rows that failed validation, with the rules they broke |
# MAGIC | `signal_silver.conversions` | Valid conversions, identifiers hashed |
# MAGIC | `signal_silver.quarantine_conversions` | Conversions that failed validation |
# MAGIC | `signal_silver.campaigns_scd2` | Full history of every campaign version (SCD Type 2) |
# MAGIC | `signal_silver.dim_stations` | Station reference data |
# MAGIC
# MAGIC **Your TODO is in the impressions section.** The notebook refuses to run past it until it's done.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

dbutils.widgets.text("repo_path", "")
# Folder holding transforms.py. Blank = this notebook's folder. In a Job, set it to the
# full path, e.g. /Workspace/Users/<you>/signal, because the working directory can differ.
sys.path.insert(0, dbutils.widgets.get("repo_path") or os.getcwd())
from transforms import (
    clean_conversions,
    clean_impressions,
    conversion_quality_flags,
    impression_quality_flags,
)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("hash_salt", "signal-demo-salt")
CATALOG = dbutils.widgets.get("catalog")
# In production the salt comes from a secret scope, never a widget or the code.
HASH_SALT = dbutils.widgets.get("hash_salt")

BRONZE, SILVER = f"{CATALOG}.signal_bronze", f"{CATALOG}.signal_silver"
CHECKPOINTS = f"/Volumes/{CATALOG}/signal_landing/checkpoints"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")


def run(df, name):
    """Stream df into SILVER.name with its own checkpoint, process what's new, then stop."""
    q = (df.writeStream
         .option("checkpointLocation", f"{CHECKPOINTS}/silver_{name}")
         .trigger(availableNow=True)
         .toTable(f"{SILVER}.{name}"))
    q.awaitTermination()
    return q

# COMMAND ----------

# MAGIC %md
# MAGIC ## Impressions
# MAGIC 1. Flag every row against the validation rules (`transforms.impression_quality_flags`).
# MAGIC 2. Failed rows go to quarantine with the list of rules they broke. Nothing is dropped silently.
# MAGIC 3. Valid rows are reshaped: IP and mobile ID become keyed hashes, geo is rounded to ~1 km.
# MAGIC 4. **Dedupe** (your TODO), then write to silver.

# COMMAND ----------

flagged = impression_quality_flags(spark.readStream.table(f"{BRONZE}.impressions"))

run(flagged.where(F.size("_dq_errors") > 0)
           .withColumn("_quarantined_at", F.current_timestamp()),
    "quarantine_impressions")

valid = clean_impressions(flagged.where(F.size("_dq_errors") == 0), HASH_SALT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO (yours): deduplicate impressions
# MAGIC The ad server sometimes delivers the same impression twice (same `impression_id`).
# MAGIC Some events also arrive up to **1 day late**, in the next day's file.
# MAGIC
# MAGIC Replace `deduped = None` with a streaming dedupe that:
# MAGIC - keys on `impression_id`
# MAGIC - uses `event_ts` as event time, with a watermark long enough to cover the 1-day lateness
# MAGIC - lets Spark drop old state once the watermark passes it
# MAGIC
# MAGIC Hints: `.withWatermark(...)` and `.dropDuplicatesWithinWatermark([...])`.
# MAGIC Be ready to explain on Monday: why not plain `dropDuplicates()`? What happens to a
# MAGIC duplicate that arrives after the watermark?

# COMMAND ----------

deduped = None  # TODO: your streaming dedupe on `valid`

assert deduped is not None, "Complete the dedupe TODO above before running the rest."
run(deduped, "impressions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversions
# MAGIC Same validate, quarantine, hash pattern. This source has no duplicate deliveries, so no
# MAGIC dedupe here. In production you'd still add one; it's cheap insurance.

# COMMAND ----------

flagged_conv = conversion_quality_flags(spark.readStream.table(f"{BRONZE}.conversions"))
run(flagged_conv.where(F.size("_dq_errors") > 0)
                .withColumn("_quarantined_at", F.current_timestamp()),
    "quarantine_conversions")
run(clean_conversions(flagged_conv.where(F.size("_dq_errors") == 0), HASH_SALT), "conversions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaigns: SCD Type 2 from the Oracle change feed
# MAGIC Each change closes the current version (`is_current = false`, `valid_to` set) and inserts
# MAGIC a new one. Changes are applied **in `updated_at` order**, because one micro-batch can hold
# MAGIC several days of changes when the backlog runs. Both steps are idempotent: a change only
# MAGIC applies if it's newer than the current version, and is never inserted twice. So a retried
# MAGIC batch can't corrupt history.

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER}.campaigns_scd2 (
    campaign_id STRING, advertiser_id STRING, campaign_name STRING, budget_usd INT,
    status STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)
""")


def apply_scd2(batch_df, batch_id):
    session = batch_df.sparkSession
    changes = (batch_df.where("campaign_id IS NOT NULL")
               .withColumn("updated_at", F.to_timestamp("updated_at")))
    for row in changes.select("updated_at").distinct().orderBy("updated_at").collect():
        changes.where(F.col("updated_at") == row.updated_at).createOrReplaceTempView("scd2_changes")
        # 1) close the current version when something actually changed
        session.sql(f"""
          MERGE INTO {SILVER}.campaigns_scd2 t
          USING scd2_changes s
          ON t.campaign_id = s.campaign_id AND t.is_current
          WHEN MATCHED AND s.updated_at > t.valid_from
                       AND (t.budget_usd <> s.budget_usd OR t.status <> s.status
                            OR t.campaign_name <> s.campaign_name
                            OR t.advertiser_id <> s.advertiser_id)
            THEN UPDATE SET t.is_current = false, t.valid_to = s.updated_at
        """)
        # 2) insert a new current version for campaigns that no longer have one (or are new)
        session.sql(f"""
          INSERT INTO {SILVER}.campaigns_scd2
          SELECT s.campaign_id, s.advertiser_id, s.campaign_name, s.budget_usd, s.status,
                 s.updated_at AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, true AS is_current
          FROM scd2_changes s
          LEFT ANTI JOIN {SILVER}.campaigns_scd2 t
            ON t.campaign_id = s.campaign_id
           AND (t.is_current OR t.valid_from >= s.updated_at)
        """)


q = (spark.readStream.table(f"{BRONZE}.campaigns_cdc")
     .writeStream
     .foreachBatch(apply_scd2)
     .option("checkpointLocation", f"{CHECKPOINTS}/silver_campaigns_scd2")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()

# COMMAND ----------

# Reference data: small and replaced wholesale, so a plain batch overwrite is the honest choice.
(spark.table(f"{BRONZE}.stations")
 .select("station_id", "station_name", "content_type", "genre", "market")
 .dropDuplicates(["station_id"])
 .write.mode("overwrite").saveAsTable(f"{SILVER}.dim_stations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check your work
# MAGIC - `silver_impressions` should be **exactly 462,000** (14 days x 33,000) and
# MAGIC   `duplicate_ids` must be **0**. If not, your dedupe isn't working.
# MAGIC - Quarantine should hold roughly 2,000-2,500 impressions; look at which rules they broke.
# MAGIC - Every campaign should have exactly one current row, and several should have history.

# COMMAND ----------

display(spark.sql(f"""
  SELECT count(*) AS silver_impressions,
         count(*) - count(DISTINCT impression_id) AS duplicate_ids,
         (SELECT count(*) FROM {SILVER}.quarantine_impressions) AS quarantined
  FROM {SILVER}.impressions
"""))

display(spark.sql(f"""
  SELECT rule, count(*) AS rows
  FROM {SILVER}.quarantine_impressions LATERAL VIEW explode(_dq_errors) e AS rule
  GROUP BY rule ORDER BY rows DESC
"""))

display(spark.sql(f"""
  SELECT campaign_id, count(*) AS versions, count_if(is_current) AS current_rows
  FROM {SILVER}.campaigns_scd2 GROUP BY campaign_id ORDER BY versions DESC
"""))
