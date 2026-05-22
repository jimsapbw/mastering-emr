# Step 12: Scale Data And Tune Runtime

## Purpose

Scale the EMR baseline from tiny smoke-test data to a larger dataset and rerun the three-step EMR pipeline.

The goal is to start collecting meaningful baseline evidence:

```text
runtime by step
shuffle pressure
skew behavior
spill
Spark stage/operator evidence
final output counts
```

The target runtime for the final tuned EMR baseline is roughly:

```text
7-8 minutes on a small EMR cluster
```

## Dataset Prefix Strategy

Keep tiny smoke-test data under:

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

Create small-scale data under:

```text
s3://aigithub-emr-2026/emr-migration-demo-small/
```

This lets us switch between:

```text
Tiny smoke testing:
  --base-prefix emr-migration-demo

Small scale testing:
  --base-prefix emr-migration-demo-small
```

The same JAR can be reused:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

## Create Small S3 Layout

Command:

```bash
BASE_PREFIX=emr-migration-demo-small \
bash scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Observed:

```text
created raw, converted, final, validation, and artifacts prefixes
under s3://aigithub-emr-2026/emr-migration-demo-small/
```

## Generate Small Data

Command:

```bash
spark-submit \
  --master local[*] \
  scripts/generate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-small \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --scale small \
  --partitions 64 \
  --mode overwrite
```

Observed:

```text
wrote advertiser
wrote koa_settings
wrote contextual
wrote bids
wrote impressions_feedback hour=10
wrote impressions_feedback hour=11
wrote matched_user_data
wrote feature_log
wrote sib
```

## Validate Small Data

Command:

```bash
spark-submit \
  --master local[*] \
  scripts/validate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-small \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Observed dataset counts:

```text
counts.bids=1000000
counts.impressions_feedback_same_hour=595000
counts.impressions_feedback_late_hour=105000
counts.impressions_feedback_total=700000
counts.contextual=200000
counts.matched_user_data=200000
counts.advertiser=10000
counts.koa_settings=10000
counts.feature_log=500000
counts.sib=100000
```

Observed join-key overlap:

```text
overlap.bids_to_impressions_bid_request_id=700000
overlap.bids_to_matched_user_data_bid_request_id=200000
overlap.bids_to_advertiser_advertiser_id=5505
overlap.bids_to_contextual_contextual_id=130035
overlap.bids_to_feature_log_user_id_hash=400200
overlap.bids_to_sib_user_id_hash=80200
overlap.bids_to_koa_settings_advertiser_campaign=5505
```

Observed late feedback:

```text
late_feedback.same_hour_rows=595000
late_feedback.late_n_plus_1_rows=105000
```

Observed skew:

```text
skew.frequency_bracket_high=200000
skew.frequency_bracket_low=800000
top 5 advertisers each have 90000 rows
top contextual keys each have 10000 rows
```

## Run Small Pipeline As EMR Steps

Use the same three classes, but change:

```text
--base-prefix emr-migration-demo-small
```

### Step 1: FeatureLogConverter

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-small-step-1-feature-log-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.FeatureLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-small",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

### Step 2: EligibleUserDataLogConverter

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-small-step-2-eligible-user-data-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.EligibleUserDataLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-small",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

### Step 3: BrbfJob

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --steps '[
    {
      "Name": "emr-migration-demo-small-step-3-brbf-job",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.BrbfJob",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-small",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

## Monitor Small Run

List steps:

```bash
aws emr list-steps \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --query 'Steps[*].[Id,Name,Status.State,Status.Timeline.StartDateTime,Status.Timeline.EndDateTime]' \
  --output table
```

Describe one step:

```bash
aws emr describe-step \
  --region us-east-1 \
  --cluster-id j-37DIRU3WHU1C5 \
  --step-id <step-id>
```

## Validate Small Final Output

After Step 3 completes:

```scala
val finalBrbfSmallPath =
  "s3://aigithub-emr-2026/emr-migration-demo-small/final/brbf/year=2026/month=05/day=21/hour=10/"

val finalBrbfSmall = spark.read.parquet(finalBrbfSmallPath)

finalBrbfSmall.count()
finalBrbfSmall.groupBy("branch").count().show(false)
finalBrbfSmall.groupBy("branch", "salt").count().orderBy("branch", "salt").show(50, false)
```

## Metrics To Capture

For each of the three EMR steps:

```text
step id
status
start time
end time
runtime
important printed counts
```

For Step 3 in Spark History Server:

```text
longest stage
shuffle read/write
memory spill
disk spill
task duration skew
SQL physical plan operators
final output count
branch counts
```

## Current Status

Completed:

```text
small S3 prefix layout: PASS
small data generation: PASS
small raw data validation: PASS
```

Next:

```text
Run the three EMR steps with --base-prefix emr-migration-demo-small.
```
