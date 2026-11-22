# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Synthetic data generator
# MAGIC
# MAGIC Writes Triton-like raw data into a Unity Catalog volume, one day at a time, so the
# MAGIC pipeline can be run incrementally and you can show "only new files get processed".
# MAGIC
# MAGIC | Folder | Simulates | Format | Quirks injected on purpose |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `impressions/` | S3 ad-impression logs (IP, mobile ID, geo) | JSON lines, one file per day | ~2% duplicates, ~3% late events, ~0.5% malformed rows |
# MAGIC | `conversions/` | Advertiser site visits / app installs / store visits | CSV, one file per day | Organic conversions with no ad exposure |
# MAGIC | `campaigns_cdc/` | Oracle campaign table change feed | CSV with `op` + `updated_at` | Budget and status updates for SCD2 |
# MAGIC | `stations/` | Reference data kept in GitHub | CSV | Loaded once |
# MAGIC
# MAGIC **Run order:** run all cells once with `days = 14` to create a backlog. Later, set
# MAGIC `start_day` to 14 and `days` to 1 to drop one "new day" and rerun the pipeline.

# COMMAND ----------

import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

# COMMAND ----------

# Parameters (widgets when running in Databricks, defaults when running locally)
try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("start_day", "0")
    dbutils.widgets.text("days", "14")
    CATALOG = dbutils.widgets.get("catalog")
    START_DAY = int(dbutils.widgets.get("start_day"))
    DAYS = int(dbutils.widgets.get("days"))
    ON_DATABRICKS = True
except NameError:
    CATALOG, START_DAY, DAYS, ON_DATABRICKS = "workspace", 0, 14, False

SCHEMA, VOLUME = "signal_landing", "raw"
BASE_DATE = datetime(2026, 11, 1)
IMPRESSIONS_PER_DAY = 33_000          # ~1M per month, matching what Triton shared
N_HOUSEHOLDS = 150_000
N_CAMPAIGNS, N_STATIONS, N_ADVERTISERS = 20, 50, 8

if ON_DATABRICKS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
else:
    BASE_PATH = os.environ.get("SIGNAL_BASE_PATH", "/tmp/signal_raw")

for sub in ["impressions", "conversions", "campaigns_cdc", "stations"]:
    os.makedirs(f"{BASE_PATH}/{sub}", exist_ok=True)
print("Writing to", BASE_PATH)

# COMMAND ----------

# Stable "world": households, stations, campaigns. Seeded so every run sees the same world.
world = random.Random(42)

CITIES = {  # name: (lat, lon)
    "Toronto": (43.6532, -79.3832), "Montreal": (45.5017, -73.5673),
    "Vancouver": (49.2827, -123.1207), "Calgary": (51.0447, -114.0719),
    "Ottawa": (45.4215, -75.6972), "New York": (40.7128, -74.0060),
    "Chicago": (41.8781, -87.6298), "Los Angeles": (34.0522, -118.2437),
}

def rand_ip(r):
    return f"{r.randint(24, 223)}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"

households = []
for _ in range(N_HOUSEHOLDS):
    city = world.choice(list(CITIES))
    lat, lon = CITIES[city]
    households.append({
        "ip": rand_ip(world),
        # ~70% of households expose a mobile advertising ID
        "maid": str(uuid.UUID(int=world.getrandbits(128))) if world.random() < 0.7 else None,
        "lat": round(lat + world.uniform(-0.15, 0.15), 6),
        "lon": round(lon + world.uniform(-0.15, 0.15), 6),
        "city": city,
    })

stations = [{
    "station_id": f"S{i:03d}",
    "station_name": f"Station {i}",
    "content_type": world.choice(["stream", "podcast"]),
    "genre": world.choice(["news", "sports", "music", "comedy", "talk"]),
    "market": world.choice(list(CITIES)),
} for i in range(1, N_STATIONS + 1)]

campaigns = [{
    "campaign_id": f"C{i:03d}",
    "advertiser_id": f"A{world.randint(1, N_ADVERTISERS):02d}",
    "campaign_name": f"Campaign {i}",
    "budget_usd": world.choice([5000, 10000, 25000, 50000]),
    "status": "ACTIVE",
} for i in range(1, N_CAMPAIGNS + 1)]
ADVERTISER_OF = {c["campaign_id"]: c["advertiser_id"] for c in campaigns}

# COMMAND ----------

def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def write_reference_and_initial_campaigns():
    """Day 0 only: station reference (GitHub) and the initial campaign load (Oracle)."""
    write_csv(f"{BASE_PATH}/stations/stations.csv", stations, list(stations[0]))
    rows = [{**c, "op": "INSERT", "updated_at": ts(BASE_DATE)} for c in campaigns]
    write_csv(f"{BASE_PATH}/campaigns_cdc/campaigns_cdc_000.csv", rows, list(rows[0]))

def write_campaign_changes(day, r):
    """A few UPDATEs per day: budget changes and status flips, for SCD2 history."""
    when = BASE_DATE + timedelta(days=day, hours=r.randint(8, 18))
    rows = []
    for c in r.sample(campaigns, k=r.randint(1, 3)):
        if r.random() < 0.6:
            c["budget_usd"] = int(c["budget_usd"] * r.choice([1.25, 1.5, 0.8]))
        else:
            c["status"] = "PAUSED" if c["status"] == "ACTIVE" else "ACTIVE"
        rows.append({**c, "op": "UPDATE", "updated_at": ts(when)})
    write_csv(f"{BASE_PATH}/campaigns_cdc/campaigns_cdc_{day:03d}.csv", rows, list(rows[0]))

def make_impression(day_start, r, late=False):
    hh = r.choice(households)
    offset = timedelta(seconds=r.randint(0, 86_399))
    event_time = day_start + offset - (timedelta(days=1) if late else timedelta(0))
    st = r.choice(stations)
    return {
        "impression_id": str(uuid.UUID(int=r.getrandbits(128))),
        "event_ts": ts(event_time),
        "campaign_id": r.choice(campaigns)["campaign_id"],
        "station_id": st["station_id"],
        "content_type": st["content_type"],
        "device": r.choice(["ios", "android", "web", "smart_speaker"]),
        "ip": hh["ip"],
        "maid": hh["maid"],
        "lat": hh["lat"],
        "lon": hh["lon"],
    }

def malformed_line(r, good):
    kind = r.choice(["broken_json", "bad_ip", "missing_id", "bad_geo"])
    if kind == "broken_json":
        return json.dumps(good)[:-12]                      # truncated record
    bad = dict(good)
    if kind == "bad_ip":
        bad["ip"] = "999.12.0.1"
    elif kind == "missing_id":
        bad["impression_id"] = None
    else:
        bad["lat"] = 123.45                                 # latitude out of range
    return json.dumps(bad)

# COMMAND ----------

pending_conversions = {}   # day -> list of conversions generated by earlier exposures

def generate_day(day):
    r = random.Random(1000 + day)                           # reproducible per day
    day_start = BASE_DATE + timedelta(days=day)

    lines = []
    for _ in range(IMPRESSIONS_PER_DAY):
        imp = make_impression(day_start, r, late=(r.random() < 0.03 and day > 0))
        lines.append(json.dumps(imp))
        if r.random() < 0.02:                               # duplicate delivery
            lines.append(json.dumps(imp))
        if r.random() < 0.005:                              # malformed row
            lines.append(malformed_line(r, imp))
        if r.random() < 0.006:                              # ad-driven conversion, 0-10 days later
            conv_day = day + r.randint(0, 10)
            conv_time = datetime.fromisoformat(imp["event_ts"]) + timedelta(
                days=conv_day - day, hours=r.randint(1, 20))
            pending_conversions.setdefault(conv_day, []).append({
                "conversion_id": str(uuid.UUID(int=r.getrandbits(128))),
                "advertiser_id": ADVERTISER_OF[imp["campaign_id"]],
                "conversion_type": r.choice(["site_visit", "app_install", "store_visit"]),
                "ip": imp["ip"],
                "maid": imp["maid"] or "",
                "event_ts": ts(conv_time),
            })
    r.shuffle(lines)
    with open(f"{BASE_PATH}/impressions/impressions_{day_start:%Y%m%d}.json", "w") as f:
        f.write("\n".join(lines) + "\n")

    # Conversions landing today: ad-driven ones plus organic noise with no exposure
    convs = pending_conversions.pop(day, [])
    for _ in range(r.randint(80, 140)):
        hh = r.choice(households)
        convs.append({
            "conversion_id": str(uuid.UUID(int=r.getrandbits(128))),
            "advertiser_id": f"A{r.randint(1, N_ADVERTISERS):02d}",
            "conversion_type": r.choice(["site_visit", "app_install", "store_visit"]),
            "ip": hh["ip"],
            "maid": hh["maid"] or "",
            "event_ts": ts(day_start + timedelta(seconds=r.randint(0, 86_399))),
        })
    write_csv(f"{BASE_PATH}/conversions/conversions_{day_start:%Y%m%d}.csv",
              convs, ["conversion_id", "advertiser_id", "conversion_type", "ip", "maid", "event_ts"])

    if day > 0:
        write_campaign_changes(day, r)
    return len(lines), len(convs)

# COMMAND ----------

# Replay earlier days silently so campaign state and pending conversions are consistent,
# then write only the requested days.
if START_DAY == 0:
    write_reference_and_initial_campaigns()
else:
    real_base = BASE_PATH
    BASE_PATH = "/tmp/signal_replay"
    for sub in ["impressions", "conversions", "campaigns_cdc", "stations"]:
        os.makedirs(f"{BASE_PATH}/{sub}", exist_ok=True)
    for d in range(START_DAY):
        generate_day(d)
    BASE_PATH = real_base

for d in range(START_DAY, START_DAY + DAYS):
    n_imp, n_conv = generate_day(d)
    print(f"day {d:>2} ({BASE_DATE + timedelta(days=d):%Y-%m-%d}): "
          f"{n_imp:,} impression lines, {n_conv:,} conversions")
