"""Reusable transformations for the SIGNAL pipeline.

Kept out of the notebooks so they can be unit tested with pytest
(see tests/test_transforms.py) and reused by every layer.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Strict IPv4: four octets, each 0-255
IPV4_REGEX = (
    r"^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}"
    r"(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
)


# --------------------------------------------------------------------------- privacy
def keyed_hash(col: Column, salt: str) -> Column:
    """SHA-256 of salt + normalised value. Same input -> same hash, so joins still work,
    but the raw identifier can't be recovered without the salt. Null/blank -> null."""
    cleaned = F.lower(F.trim(col))
    return F.when(cleaned.isNull() | (cleaned == ""), F.lit(None)).otherwise(
        F.sha2(F.concat(F.lit(salt), cleaned), 256)
    )


# --------------------------------------------------------------------------- data quality
def _errors(rules: dict) -> Column:
    """Array of the names of the rules that failed for a row (empty array = clean)."""
    flagged = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return F.filter(flagged, lambda x: x.isNotNull())


def _bad_ip(c: Column) -> Column:
    return F.coalesce(~c.rlike(IPV4_REGEX), F.lit(True))


def impression_quality_flags(df: DataFrame) -> DataFrame:
    """Adds `_dq_errors`: which validation rules each impression row failed."""
    rules = {
        "corrupt_record": F.col("_corrupt_record").isNotNull(),
        "missing_impression_id": F.col("impression_id").isNull(),
        "bad_event_ts": F.to_timestamp("event_ts").isNull(),
        "bad_ip": _bad_ip(F.col("ip")),
        "bad_geo": F.coalesce(
            ~(F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180)), F.lit(True)
        ),
    }
    return df.withColumn("_dq_errors", _errors(rules))


def conversion_quality_flags(df: DataFrame) -> DataFrame:
    """Adds `_dq_errors` for conversion rows."""
    rules = {
        "rescued_data": F.col("_rescued_data").isNotNull(),
        "missing_conversion_id": F.col("conversion_id").isNull(),
        "bad_event_ts": F.to_timestamp("event_ts").isNull(),
        "bad_ip": _bad_ip(F.col("ip")),
    }
    return df.withColumn("_dq_errors", _errors(rules))


# --------------------------------------------------------------------------- silver shaping
def clean_impressions(df: DataFrame, salt: str) -> DataFrame:
    """Valid impression rows -> silver shape. Raw IP, mobile ID and exact geo are dropped."""
    return df.select(
        "impression_id",
        F.to_timestamp("event_ts").alias("event_ts"),
        F.to_date(F.to_timestamp("event_ts")).alias("event_date"),
        "campaign_id",
        "station_id",
        "content_type",
        "device",
        keyed_hash(F.col("ip"), salt).alias("ip_hash"),
        keyed_hash(F.col("maid"), salt).alias("maid_hash"),
        F.round("lat", 2).alias("lat_2dp"),   # ~1 km precision is enough for reporting
        F.round("lon", 2).alias("lon_2dp"),
        "_source_file",
        "_ingest_ts",
    )


def clean_conversions(df: DataFrame, salt: str) -> DataFrame:
    return df.select(
        "conversion_id",
        F.to_timestamp("event_ts").alias("event_ts"),
        F.to_date(F.to_timestamp("event_ts")).alias("event_date"),
        "advertiser_id",
        "conversion_type",
        keyed_hash(F.col("ip"), salt).alias("ip_hash"),
        keyed_hash(F.col("maid"), salt).alias("maid_hash"),
        "_source_file",
        "_ingest_ts",
    )


# --------------------------------------------------------------------------- attribution
def last_touch_attribution(exposures: DataFrame, conversions: DataFrame,
                           lookback_days: int = 7) -> DataFrame:
    """Credit each conversion to the most recent matching ad exposure.

    exposures:   impression_id, event_ts, campaign_id, advertiser_id, ip_hash, maid_hash
    conversions: conversion_id, event_ts, advertiser_id, conversion_type, ip_hash, maid_hash

    A conversion matches an exposure when ALL of these hold:
      1. same advertiser_id
      2. the exposure happened BEFORE the conversion
      3. the exposure happened no more than `lookback_days` before the conversion
      4. same maid_hash (when the conversion has one) OR same ip_hash

    Return ONE row per attributed conversion (conversions with no match are left out):
      conversion_id, conversion_ts, conversion_type, advertiser_id,
      campaign_id, impression_id, exposure_ts,
      match_key        -> "maid" if the mobile IDs matched, otherwise "ip"
      hours_to_convert -> (conversion_ts - exposure_ts) in hours, as a double

    If several exposures match, keep the LATEST one (break ties on impression_id, descending).

    TODO (yours): implement this. Hints:
      - alias the inputs: e = exposures.alias("e"), c = conversions.alias("c")
      - build one join condition with &, | and F.expr(f"INTERVAL {lookback_days} DAYS")
      - select/rename the columns you need, THEN rank with
        Window.partitionBy("conversion_id").orderBy(...desc...) and F.row_number()
      - run 04_run_tests: the attribution tests are your spec
    """
    raise NotImplementedError("TODO: implement last_touch_attribution")
