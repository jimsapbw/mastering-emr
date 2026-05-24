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
small EMR Step 1 FeatureLogConverter: COMPLETED
small EMR Step 2 EligibleUserDataLogConverter: COMPLETED
small EMR Step 3 BrbfJob: COMPLETED
small Spark History troubleshooting: COMPLETED
small conclusion: Stage 38 / Job 22 / SQL Query 8 / count at BrbfJob.scala:80
small bottleneck classification: too many small shuffle partitions/task waves, not primary skew or memory pressure
```

Medium controlled baseline checkpoint:

```text
medium prefix:
  s3://aigithub-emr-2026/emr-migration-demo-medium/

MCBP #1 medium scale definition: PASS
MCBP #2 medium S3 prefix layout: PASS
MCBP #3 medium raw data generation: PASS
MCBP #4 medium raw data validation: PASS
MCBP #5 medium Step 1 FeatureLogConverter: COMPLETED
  step id: s-05377242F3A87LPS5JUZ
MCBP #6 medium Step 2 EligibleUserDataLogConverter: COMPLETED
  step id: s-04508423OSLJSZBQW90K

MCBP #7 medium Step 3 BrbfJob attempt 1: FAILED
  cluster id: j-3S62AU5IR98MM
  step id: s-0661117QH89RUU39ZQ1
  application id: application_1779541486316_0010
  job/stage: Job 20 / Stage 37.0
  action: count at BrbfJob.scala:80
  root cause: java.io.IOException: No space left on device

MCBP #7 medium Step 3 BrbfJob attempt 2: FAILED
  cluster id: j-3S62AU5IR98MM
  step id: s-09085642ZQTGXNML2TKJ
  application id: application_1779541486316_0011
  controlled change: EMR scale-out plus --executor-cores 2
  live UI reached: Job 20 / Stage 38
  observed input: about 11.8 GiB over 200 tasks
  result: failed before completing medium Step 3
```

Current decision:

```text
Stop tuning blindly on the current cluster.
Create a restore checkpoint before infrastructure changes.
Create a new EMR cluster with larger worker local/EBS disk from the start.
Rerun only medium Step 3 first, because medium Steps 1 and 2 already wrote converted data to S3.
```

## Next Recommended Action

Before creating or changing infrastructure, create a restore checkpoint:

```bash
aws s3 sync . \
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ \
  --region us-east-1
```

If creating a zip backup from the repo root:

```bash
zip -r /mnt/tmp/emr_migration_demo_2026-05-24_mcbp_medium_disk_checkpoint.zip . \
  -x '.git/*' \
  -x 'spark/target/*'
```

Upload the zip:

```bash
aws s3 cp \
  /mnt/tmp/emr_migration_demo_2026-05-24_mcbp_medium_disk_checkpoint.zip \
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo_2026-05-24_mcbp_medium_disk_checkpoint.zip \
  --region us-east-1
```

Because `BrbfJob.scala` now lets `spark-submit --conf` override its baseline Spark defaults, rebuild and upload the JAR before the new-cluster rerun:

```bash
cd spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
aws s3 cp target/*.jar s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/ --region us-east-1
```

Recommended new cluster shape:

```text
EMR release:
  Same as current if possible: EMR 7.13.0 / Spark 3.5.6-amzn-2

Primary node:
  1 node, same or similar instance type

Core nodes:
  4 nodes

Task nodes:
  0 for first rerun

Purchasing:
  On-Demand for the baseline; avoid Spot for the first rerun

Worker instance type:
  r8g.xlarge

Worker EBS/root disk:
  gp3
  300 GiB
  3000 IOPS
  125 MiB/s throughput
  1 volume per instance

Managed scaling:
  enabled, but start with enough core nodes

Max cluster size:
  6 instances

Max core nodes:
  4 instances

Max On-Demand:
  6 instances

Spark event log:
  enabled

Spark history dir:
  hdfs:///var/log/spark/apps
  or S3 if history must survive cluster termination
```

Why this shape:

```text
Medium Step 3 failed from worker local disk exhaustion during shuffle/spill, not just lack of CPU.
The old cluster was not failing because the Scala code was wrong.
The failing path creates a large joined/cached/shuffled pipeline at BrbfJob.scala:80.
Bigger worker disks plus more distributed executors should give Spark enough local spill/shuffle room to complete.
```

After the restore checkpoint exists and the new JAR is uploaded, create the replacement EMR cluster with larger worker local/EBS disk, then rerun only medium Step 3.

Do not rerun medium Step 1 or Step 2 first; they already completed and wrote converted data to:

```text
s3://aigithub-emr-2026/emr-migration-demo-medium/
```

Use this first-rerun Step 3 command style:

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id <new-cluster-id> \
  --steps '[
    {
      "Name": "emr-migration-demo-medium-step-3-brbf-job-disk-baseline",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--conf", "spark.sql.shuffle.partitions=400",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.shuffle.compress=true",
        "--conf", "spark.shuffle.spill.compress=true",
        "--class", "com.demo.emr.BrbfJob",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-medium",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Preserve the same analysis discipline on the new run:

```text
1. Capture application id and step id.
2. Confirm runtime configuration.
3. Watch Job 20 / count at BrbfJob.scala:80 first.
4. If Step 3 completes, validate final output and run the Spark UI prompt sequence.
5. If Step 3 fails again, capture the first root-cause error before changing more settings.
6. Keep the live Spark UI tunnel ready so Job 20 / Stage 37 or Stage 38 metrics can be captured during the run.
```
