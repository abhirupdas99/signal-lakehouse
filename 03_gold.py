# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: one silver layer, two shapes
# MAGIC
# MAGIC | Table | Team | Question it answers |
# MAGIC | --- | --- | --- |
# MAGIC | `signal_gold_analytics.campaign_daily_delivery` | Analytics | How many impressions and households did each campaign reach per day? |
# MAGIC | `signal_gold_ds.attribution_last_touch` | Data Science | Which ad exposure gets credit for each conversion? |
# MAGIC | `signal_gold_analytics.campaign_attribution_summary` | Analytics | Attributed conversions per campaign |
# MAGIC | `signal_gold_ds.advertiser_lift` | Data Science | Do exposed households convert more than unexposed ones? |
# MAGIC
# MAGIC Gold is rebuilt in batch here because it's small. In production these would be
# MAGIC materialized views refreshed incrementally.
# MAGIC
# MAGIC **Your TODO:** implement `last_touch_attribution` in `transforms.py`, using
# MAGIC `tests/test_transforms.py` as the spec. Run `04_run_tests` until all tests pass, then run this.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("repo_path", "")
dbutils.widgets.text("lookback_days", "7")
CATALOG = dbutils.widgets.get("catalog")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
sys.path.insert(0, dbutils.widgets.get("repo_path") or os.getcwd())

import importlib
import transforms
importlib.reload(transforms)   # pick up your latest edits without restarting Python
from transforms import last_touch_attribution

SILVER = f"{CATALOG}.signal_silver"
GOLD_AN, GOLD_DS = f"{CATALOG}.signal_gold_analytics", f"{CATALOG}.signal_gold_ds"
for schema in (GOLD_AN, GOLD_DS):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

impressions = spark.table(f"{SILVER}.impressions")
conversions = spark.table(f"{SILVER}.conversions")
campaigns_now = spark.table(f"{SILVER}.campaigns_scd2").where("is_current")
stations = spark.table(f"{SILVER}.dim_stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics: daily delivery per campaign
# MAGIC Reach = distinct households (hashed IPs). Analysts get reach without ever seeing an IP.

# COMMAND ----------

delivery = (
    impressions
    .join(campaigns_now.select("campaign_id", "advertiser_id", "campaign_name"), "campaign_id")
    .join(stations.select("station_id", "genre"), "station_id", "left")
    .groupBy("event_date", "advertiser_id", "campaign_id", "campaign_name")
    .agg(F.count("*").alias("impressions"),
         F.countDistinct("ip_hash").alias("households_reached"),
         F.countDistinct("station_id").alias("stations"),
         F.round(F.avg(F.when(F.col("content_type") == "podcast", 1).otherwise(0)), 3)
          .alias("podcast_share"))
)
delivery.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_AN}.campaign_daily_delivery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science: last-touch attribution
# MAGIC Exposures need the advertiser, which lives on the campaign. We use the campaign's
# MAGIC **current** advertiser; advertiser never changes in this data, so that's safe here.
# MAGIC If it could change, you'd join to the SCD2 version valid at `event_ts`.

# COMMAND ----------

exposures = impressions.join(
    campaigns_now.select("campaign_id", "advertiser_id"), "campaign_id"
).select("impression_id", "event_ts", "campaign_id", "advertiser_id", "ip_hash", "maid_hash")

attribution = last_touch_attribution(
    exposures,
    conversions.select("conversion_id", "event_ts", "advertiser_id", "conversion_type",
                       "ip_hash", "maid_hash"),
    LOOKBACK_DAYS,
)
attribution.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DS}.attribution_last_touch")

summary = (
    spark.table(f"{GOLD_DS}.attribution_last_touch")
    .groupBy("advertiser_id", "campaign_id")
    .agg(F.count("*").alias("attributed_conversions"),
         F.count_if(F.col("match_key") == "maid").alias("matched_on_mobile_id"),
         F.round(F.percentile_approx("hours_to_convert", 0.5), 1).alias("median_hours_to_convert"))
)
summary.write.mode("overwrite").saveAsTable(f"{GOLD_AN}.campaign_attribution_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science: naive lift per advertiser
# MAGIC Conversion rate of households that heard the advertiser's ads vs. households that didn't.
# MAGIC **This is not causal.** Exposed households may differ from unexposed ones (selection bias).
# MAGIC A real lift study uses a randomized holdout group. Say exactly that on Monday.

# COMMAND ----------

universe = impressions.select("ip_hash").distinct()
exposed = exposures.select("advertiser_id", "ip_hash").distinct()
converters = conversions.select("advertiser_id", "ip_hash").distinct()
advertisers = exposures.select("advertiser_id").distinct()

hh = (advertisers.crossJoin(universe)
      .join(exposed.withColumn("exposed", F.lit(1)), ["advertiser_id", "ip_hash"], "left")
      .join(converters.withColumn("converted", F.lit(1)), ["advertiser_id", "ip_hash"], "left")
      .fillna(0, ["exposed", "converted"]))

lift = (hh.groupBy("advertiser_id")
        .agg(F.sum("exposed").alias("exposed_households"),
             F.sum(F.col("converted") * F.col("exposed")).alias("exposed_converters"),
             F.sum(1 - F.col("exposed")).alias("unexposed_households"),
             F.sum(F.col("converted") * (1 - F.col("exposed"))).alias("unexposed_converters"))
        .withColumn("rate_exposed", F.col("exposed_converters") / F.col("exposed_households"))
        .withColumn("rate_unexposed", F.col("unexposed_converters") / F.col("unexposed_households"))
        .withColumn("naive_lift_pct",
                    F.round((F.col("rate_exposed") / F.col("rate_unexposed") - 1) * 100, 1)))
lift.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DS}.advertiser_lift")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check your work
# MAGIC - Roughly half of the ~3,300 conversions should be attributed. The rest are organic
# MAGIC   or fall outside the 7-day window.
# MAGIC - Change `lookback_days` to 3 and rerun: attributed conversions should drop. Why?

# COMMAND ----------

display(spark.sql(f"""
  SELECT (SELECT count(*) FROM {SILVER}.conversions) AS total_conversions,
         count(*) AS attributed,
         round(count(*) / (SELECT count(*) FROM {SILVER}.conversions) * 100, 1) AS attributed_pct,
         count_if(match_key = 'maid') AS via_mobile_id,
         count_if(match_key = 'ip') AS via_ip
  FROM {GOLD_DS}.attribution_last_touch
"""))
display(spark.table(f"{GOLD_DS}.advertiser_lift").orderBy(F.desc("naive_lift_pct")))
