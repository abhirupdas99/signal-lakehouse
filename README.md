# SIGNAL — Audio ad attribution lakehouse on Databricks

A scaled-down build of an automated ingestion and attribution platform for audio advertising
data: ad impressions carrying IP addresses, mobile advertising IDs and geo coordinates,
advertiser conversions, an Oracle-style campaign change feed, and reference data from GitHub.

Built on Databricks (AWS, serverless) with Auto Loader, Structured Streaming (`availableNow`),
Delta Lake and Unity Catalog.

## Architecture

```
Landing volume (S3)           Bronze                 Silver                          Gold
impressions/*.json   ──▶  impressions      ──▶  impressions (deduped, hashed) ──┬─▶ analytics.campaign_daily_delivery
conversions/*.csv    ──▶  conversions      ──▶  conversions (hashed)          ──┼─▶ ds.attribution_last_touch
campaigns_cdc/*.csv  ──▶  campaigns_cdc    ──▶  campaigns_scd2 (SCD Type 2)   ──┼─▶ analytics.campaign_attribution_summary
stations/*.csv       ──▶  stations         ──▶  dim_stations                  ──┘─▶ ds.advertiser_lift
                                          └──▶  quarantine_* (failed rows + reasons)
```

## Design decisions

- **Structured Streaming at ~1M records/month, on purpose.** Not for latency: checkpoints
  guarantee each file and row is processed exactly once. With `availableNow` the job processes
  only new data and stops, so it costs the same as batch. Switching to continuous is one setting.
- **Explicit schemas as data contracts.** No inference. Malformed JSON lands in
  `_corrupt_record`, CSV type mismatches in `_rescued_data`.
- **Nothing is dropped silently.** Rows failing validation go to quarantine tables along with
  the names of the rules they broke.
- **Privacy by default.** IP addresses and mobile IDs are replaced by salted SHA-256 hashes in
  silver, so joins still work but raw identifiers never reach analysts or models. Geo is
  rounded to ~1 km. Raw values stay only in restricted bronze.
- **Streaming dedupe with a watermark** (`dropDuplicatesWithinWatermark`) handles duplicate
  deliveries and events arriving up to a day late, with bounded state.
- **SCD Type 2 for campaigns**, applied in change order and idempotent, so retried batches
  can't corrupt history.
- **One silver layer, two gold shapes:** marts for Analytics, attribution and lift datasets
  for Data Science.
- **Tested transformations.** Logic lives in `transforms.py` with pytest unit tests.

## Run it

1. Import the repo as a Databricks Git folder (Free Edition works).
2. `00_generate_data`: creates 14 days of synthetic data in a Unity Catalog volume.
3. `01_bronze` → `02_silver` → `03_gold`, or the Lakeflow Job that chains them.
4. `04_run_tests`: unit tests.
5. Incremental demo: run `00_generate_data` with `start_day=14, days=1`, rerun the pipeline,
   and watch only the new file get processed.

## Known simplifications

- Naive lift compares exposed and unexposed households. It isn't causal; a real study
  needs a randomized holdout.
- Gold is rebuilt in batch for simplicity. In production: materialized views with incremental refresh.
- The hash salt is a notebook parameter here. In production it comes from a secret scope.
