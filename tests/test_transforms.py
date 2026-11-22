"""Unit tests for transforms.py. Run with 04_run_tests in Databricks, or `pytest` locally."""
import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from transforms import (  # noqa: E402
    clean_impressions,
    impression_quality_flags,
    keyed_hash,
    last_touch_attribution,
)

SALT = "test-salt"


@pytest.fixture(scope="session")
def spark():
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.master("local[1]").appName("signal-tests").getOrCreate()


def ts(s):
    return datetime.fromisoformat(s)


# --------------------------------------------------------------------------- keyed_hash
def test_keyed_hash_is_deterministic_salted_and_normalised(spark):
    df = spark.createDataFrame([("1.2.3.4",), (" 1.2.3.4 ",), ("5.6.7.8",)], ["ip"])
    out = [r.h for r in df.select(keyed_hash(F.col("ip"), SALT).alias("h")).collect()]
    assert out[0] == out[1]                     # trimmed input hashes the same
    assert out[0] != out[2]
    other_salt = df.select(keyed_hash(F.col("ip"), "other").alias("h")).first().h
    assert other_salt != out[0]                 # different salt, different hash
    assert len(out[0]) == 64


def test_keyed_hash_null_and_blank_stay_null(spark):
    df = spark.createDataFrame([(None,), ("",), ("   ",)], "maid string")
    out = [r.h for r in df.select(keyed_hash(F.col("maid"), SALT).alias("h")).collect()]
    assert out == [None, None, None]


# --------------------------------------------------------------------------- quality flags
IMP_SCHEMA = ("impression_id string, event_ts string, campaign_id string, station_id string, "
              "content_type string, device string, ip string, maid string, lat double, "
              "lon double, _corrupt_record string, _source_file string, _ingest_ts timestamp")


def imp(**overrides):
    row = dict(impression_id="i1", event_ts="2026-11-01T10:00:00", campaign_id="C001",
               station_id="S001", content_type="stream", device="ios", ip="10.0.0.1",
               maid="m1", lat=43.65, lon=-79.38, _corrupt_record=None,
               _source_file="f.json", _ingest_ts=None)
    row.update(overrides)
    return row


def test_quality_flags_catch_each_bad_row(spark):
    rows = [
        imp(),                                           # clean
        imp(impression_id="i2", ip="999.12.0.1"),        # bad_ip
        imp(impression_id="i3", lat=123.45),             # bad_geo
        imp(impression_id=None),                         # missing id
        imp(impression_id="i5", event_ts="not-a-date"),  # bad ts
        dict(imp(), **{k: None for k in ["impression_id", "event_ts", "ip", "lat", "lon"]},
             _corrupt_record='{"impression_id": "x'),    # corrupt line
    ]
    df = impression_quality_flags(spark.createDataFrame(rows, IMP_SCHEMA))
    errors = [set(r._dq_errors) for r in df.collect()]
    assert errors[0] == set()
    assert "bad_ip" in errors[1]
    assert "bad_geo" in errors[2]
    assert "missing_impression_id" in errors[3]
    assert "bad_event_ts" in errors[4]
    assert "corrupt_record" in errors[5]


def test_clean_impressions_drops_raw_identifiers(spark):
    df = clean_impressions(spark.createDataFrame([imp()], IMP_SCHEMA), SALT)
    assert {"ip", "maid", "lat", "lon"}.isdisjoint(df.columns)
    row = df.first()
    assert row.ip_hash is not None and row.maid_hash is not None
    assert row.lat_2dp == 43.65 and str(row.event_date) == "2026-11-01"


# --------------------------------------------------------------------------- attribution
EXP_SCHEMA = ("impression_id string, event_ts timestamp, campaign_id string, "
              "advertiser_id string, ip_hash string, maid_hash string")
CONV_SCHEMA = ("conversion_id string, event_ts timestamp, advertiser_id string, "
               "conversion_type string, ip_hash string, maid_hash string")


def attribute(spark, exposures, conversions, lookback_days=7):
    e = spark.createDataFrame(exposures, EXP_SCHEMA)
    c = spark.createDataFrame(conversions, CONV_SCHEMA)
    rows = last_touch_attribution(e, c, lookback_days).collect()
    return {r.conversion_id: r for r in rows}


def test_attribution_picks_latest_exposure_inside_window(spark):
    out = attribute(spark,
        [("i_old", ts("2026-11-01T10:00:00"), "C1", "A1", "ip1", None),
         ("i_new", ts("2026-11-03T10:00:00"), "C2", "A1", "ip1", None)],
        [("v1", ts("2026-11-04T10:00:00"), "A1", "site_visit", "ip1", None)])
    assert out["v1"].impression_id == "i_new"
    assert out["v1"].campaign_id == "C2"
    assert out["v1"].match_key == "ip"
    assert out["v1"].hours_to_convert == pytest.approx(24.0)


def test_attribution_ignores_exposures_outside_window_or_after_conversion(spark):
    out = attribute(spark,
        [("i_too_old", ts("2026-10-20T10:00:00"), "C1", "A1", "ip1", None),
         ("i_after", ts("2026-11-05T10:00:00"), "C1", "A1", "ip1", None)],
        [("v1", ts("2026-11-04T10:00:00"), "A1", "site_visit", "ip1", None)])
    assert "v1" not in out


def test_attribution_matches_on_mobile_id_when_ip_differs(spark):
    out = attribute(spark,
        [("i1", ts("2026-11-03T10:00:00"), "C1", "A1", "ip_home", "m1")],
        [("v1", ts("2026-11-03T18:00:00"), "A1", "app_install", "ip_cell", "m1")])
    assert out["v1"].impression_id == "i1"
    assert out["v1"].match_key == "maid"


def test_attribution_requires_same_advertiser(spark):
    out = attribute(spark,
        [("i1", ts("2026-11-03T10:00:00"), "C1", "A2", "ip1", None)],
        [("v1", ts("2026-11-04T10:00:00"), "A1", "site_visit", "ip1", None)])
    assert out == {}


def test_attribution_returns_one_row_per_conversion(spark):
    e = spark.createDataFrame(
        [("i1", ts("2026-11-03T10:00:00"), "C1", "A1", "ip1", "m1"),
         ("i2", ts("2026-11-03T10:00:00"), "C1", "A1", "ip1", "m1")], EXP_SCHEMA)
    c = spark.createDataFrame(
        [("v1", ts("2026-11-04T10:00:00"), "A1", "site_visit", "ip1", "m1")], CONV_SCHEMA)
    rows = last_touch_attribution(e, c).collect()
    assert len(rows) == 1
    assert rows[0].impression_id == "i2"      # tie on time -> highest impression_id
