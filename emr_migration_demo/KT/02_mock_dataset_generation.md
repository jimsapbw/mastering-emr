# Step 02: Mock Dataset Generation

## Purpose

Generate BRBF-style mock datasets for the EMR read operation.

The data is written as Snappy-compressed Parquet into the raw S3 prefixes created in Step 01.

## Generator

```text
emr_migration_demo/scripts/generate_mock_data.py
```

## Why PySpark For This Step

The demo environment has `spark-submit` available, but does not currently have `sbt`, `mvn`, or a standalone `scala` command installed.

For the setup data generator, PySpark lets us create distributed Parquet datasets in S3 immediately. The main BRBF workload can still be implemented later as a Scala Spark application packaged as a JAR.

## Datasets Created

```text
raw/bids/
raw/impressions_feedback/
raw/contextual/
raw/matched_user_data/
raw/advertiser/
raw/koa_settings/
raw/feature_log/
raw/sib/
```

## Key Joins

```text
bids.bid_request_id            -> impressions_feedback.bid_request_id
bids.bid_request_id            -> matched_user_data.bid_request_id
bids.advertiser_id             -> advertiser.advertiser_id
bids.advertiser_id/campaign_id -> koa_settings.advertiser_id/campaign_id
bids.contextual_id             -> contextual.contextual_id
bids.user_id_hash              -> feature_log.user_id_hash
bids.user_id_hash              -> sib.user_id_hash
```

## Scale Presets

```text
tiny:
  bids: 100,000
  impressions_feedback: 70,000
  feature_log: 50,000

small:
  bids: 1,000,000
  impressions_feedback: 700,000
  feature_log: 500,000

medium:
  bids: 10,000,000
  impressions_feedback: 7,000,000
  feature_log: 3,000,000
```

The first run should use `tiny` as a smoke test. Later runs can use `small` or `medium` to tune the EMR baseline runtime.

## Command

```bash
spark-submit \
  --master local[*] \
  emr_migration_demo/scripts/generate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --scale tiny \
  --partitions 32 \
  --mode overwrite
```

## Executed Smoke Run

The first mock data run used the `tiny` scale preset:

```text
bids: 100,000
impressions_feedback: 70,000 total
contextual: 20,000
matched_user_data: 20,000
advertiser: 1,000
koa_settings: 1,000
feature_log: 50,000
sib: 20,000
```

Spark completed successfully and wrote all 8 raw datasets to S3.

## Verification Commands

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/raw/bids/year=2026/month=05/day=21/hour=10/ --region us-east-1
```

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/raw/impressions_feedback/year=2026/month=05/day=21/hour=11/ --region us-east-1
```

```bash
aws s3 ls s3://aigithub-emr-2026/emr-migration-demo/raw/feature_log/year=2026/month=05/day=21/hour=10/ --region us-east-1
```

## Observed Verification

The checked prefixes contain:

```text
_SUCCESS
part-*.snappy.parquet
```

For the smoke run, each checked prefix had 32 Parquet part files because the command used:

```text
--partitions 32
```

## Skew Design

The generator intentionally creates skew so the EMR baseline can reproduce the before-DAG problem:

- 20% of bid rows go to a small set of hot users.
- 45% of bid rows go to the top 5 advertisers.
- 35% of bid rows go to hot contextual IDs.
- impression feedback is split between same-hour and `N+1` late-hour data.

This prepares the later BRBF job to show expensive shuffles, manual salting, branch union, and sort pressure.
