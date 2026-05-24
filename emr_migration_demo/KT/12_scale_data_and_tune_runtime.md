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

## Create Smalllevel2 10-Minute Calibration Load

Use this after the full medium Step 3 cancellation. The goal is a practical
client-demo load that is larger than `small`, but stays close enough to the
known-good scale to target about 10 minutes on the larger replacement cluster.

Smalllevel2 scale:

```text
bids: 1,500,000
impressions_feedback: 1,050,000
contextual: 300,000
matched_user_data: 300,000
advertiser: 12,000
koa_settings: 12,000
feature_log: 750,000
sib: 150,000
```

Create the S3 layout:

```bash
BASE_PREFIX=emr-migration-demo-smalllevel2 \
bash scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Generate raw data:

```bash
spark-submit \
  --master local[*] \
  scripts/generate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-smalllevel2 \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --scale smalllevel2 \
  --partitions 96 \
  --mode overwrite
```

Validate raw data:

```bash
spark-submit \
  --master local[*] \
  scripts/validate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-smalllevel2 \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Submit all three smalllevel2 EMR steps on the replacement cluster:

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-3O78ZN9EMO9W2 \
  --steps '[
    {
      "Name": "emr-migration-demo-smalllevel2-step-1-feature-log-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.FeatureLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-smalllevel2",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    },
    {
      "Name": "emr-migration-demo-smalllevel2-step-2-eligible-user-data-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.EligibleUserDataLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-smalllevel2",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    },
    {
      "Name": "emr-migration-demo-smalllevel2-step-3-brbf-job-10min-calibration",
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
        "--base-prefix", "emr-migration-demo-smalllevel2",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Monitor:

```bash
aws emr list-steps \
  --region us-east-1 \
  --cluster-id j-3O78ZN9EMO9W2 \
  --query 'Steps[*].[Id,Name,Status.State,Status.Timeline.StartDateTime,Status.Timeline.EndDateTime]' \
  --output table
```

Observed smalllevel2 result:

```text
BRBF Step 3 completed in about 4 minutes on the replacement cluster.
Interpretation: smalllevel2 is stable and useful as a safe baseline, but it is
below the desired 10-minute BRBF troubleshooting target.
Next recommended calibration load: 3x light-medium.
```

3x light-medium target row counts:

```text
bids: 3,000,000
impressions_feedback: 2,100,000
contextual: 500,000
matched_user_data: 500,000
advertiser: 15,000
koa_settings: 15,000
feature_log: 1,000,000
sib: 250,000
```

## Create Light-Medium 3x Demo Load

Use this after smalllevel2 finishes too quickly for the desired demo window.
This is the preferred dataset for the current replacement cluster because
smalllevel2 completed BRBF Step 3 in about 4 minutes and light-medium completed
BRBF Step 3 in about 12 minutes.

Create the S3 layout:

```bash
BASE_PREFIX=emr-migration-demo-light-medium \
bash scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Generate raw data:

```bash
spark-submit \
  --master local[*] \
  scripts/generate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-light-medium \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --scale lightmedium \
  --partitions 128 \
  --mode overwrite
```

Validate raw data:

```bash
spark-submit \
  --master local[*] \
  scripts/validate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo-light-medium \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Submit all three light-medium EMR steps:

```bash
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-3O78ZN9EMO9W2 \
  --steps '[
    {
      "Name": "emr-migration-demo-light-medium-step-1-feature-log-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.FeatureLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-light-medium",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    },
    {
      "Name": "emr-migration-demo-light-medium-step-2-eligible-user-data-converter",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--class", "com.demo.emr.EligibleUserDataLogConverter",
        "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
        "--bucket", "aigithub-emr-2026",
        "--base-prefix", "emr-migration-demo-light-medium",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    },
    {
      "Name": "emr-migration-demo-light-medium-step-3-brbf-job-demo-calibration",
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
        "--base-prefix", "emr-migration-demo-light-medium",
        "--run-date", "2026-05-21",
        "--hour", "10",
        "--late-hour", "11",
        "--output-mode", "overwrite"
      ]
    }
  ]'
```

Monitor:

```bash
aws emr list-steps \
  --region us-east-1 \
  --cluster-id j-3O78ZN9EMO9W2 \
  --query 'Steps[*].[Id,Name,Status.State,Status.Timeline.StartDateTime,Status.Timeline.EndDateTime]' \
  --output table
```

Observed light-medium result:

```text
BRBF Step 3 completed in about 12 minutes on the replacement cluster.
Interpretation: this is the preferred demo load for cluster j-3O78ZN9EMO9W2.
It is long enough to show useful Spark UI behavior and short enough for a
client walkthrough.
```

Completed light-medium Spark UI analysis:

```text
app id: application_1779641349593_0009
EMR step id: s-06984631PV9G98A51OTN
dominant query: Query 8 / count at BrbfJob.scala:80
query duration: 7.5 minutes
dominant stage: Stage 63 / Job 38
stage duration: 7.1 minutes

primary finding:
  feature-log ShuffledHashJoin expands about 3,000,000 joined rows to
  602,400,000 rows.

stage classification:
  Stage 63 has task-duration stragglers:
    median task duration: 95 ms
    p75 task duration: 21 s
    max task duration: 1.5 min
  Shuffle byte skew is only mild to moderate from the pasted metrics.
  Memory pressure and spill are not proven as primary from the pasted metrics.

code mapping:
  BrbfJob.scala:80 -> joined.count()
  BrbfJob.scala:51-78 -> joined DataFrame construction and persist
  BrbfJob.scala:57-62 -> featureLog join on user_id_hash/contextual_id
  FeatureLogConverter.scala:38 -> deduplicates by feature_event_id only

Databricks migration recommendation:
  validate whether the feature-log many-to-many fanout is expected.
  If accidental, correct the join grain or pre-aggregate/deduplicate/window
  feature_log before the BRBF join.
  If intentional, use Delta layout/statistics, AQE/skew handling, Photon, and
  reduce repeated actions over the expanded joined DataFrame.

Photon expectation with code unchanged:
  plan for about 1.2x-2x end-to-end improvement;
  treat 2x-3x as upside, not a commitment;
  do not promise 5x because Photon cannot remove the 602.4M-row fanout.
```

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

Preflight:

```bash
aws sts get-caller-identity --region us-east-1
mvn -version
```

If Maven is not installed on the active machine or EMR primary:

```bash
sudo dnf install -y maven
```

Build and upload:

```bash
cd spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
aws s3 cp target/*.jar s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/ --region us-east-1
```

Implementation guardrail:

```text
Do not skip the rebuild/upload step.
The source now uses setDefaultSparkConf(...), but the S3 JAR must contain that change before
the medium Step 3 submit-time overrides will take effect.
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

Active replacement cluster selected:

```text
Cluster name: itv-github-dev-cluster
Cluster ID: j-3O78ZN9EMO9W2
Region: us-east-1
EMR release: emr-7.13.0
Spark version: 3.5.6
Status when recorded: Waiting
Capacity: 1 Primary, 4 Core, 0 Task
Primary public DNS: ec2-100-55-172-52.compute-1.amazonaws.com
Core instance type: r8g.xlarge
Core EBS: 300 GiB, 1 volume per core node
Purchasing: On-Demand
Managed scaling: enabled, 4-6 instances, max 4 core nodes
CloudWatch log group: /aws/emr/j-3O78ZN9EMO9W2
```

Environment-prep boundary:

```text
Prepare credentials, Maven, rebuilt JAR, and S3 upload first.
Do not submit medium Step 3 until environment prep is complete.
```

Current environment-prep status:

```text
Docs updated with active cluster: COMPLETE
Local Maven installed: COMPLETE
Local JAR rebuild: COMPLETE
Local JAR path: spark/target/emr-migration-demo-0.1.0.jar
JAR contents verified: BrbfJob, FeatureLogConverter, EligibleUserDataLogConverter present
AWS credential check: COMPLETE, account 210570462212
Cluster describe: COMPLETE, j-3O78ZN9EMO9W2 WAITING on emr-7.13.0
S3 JAR upload: COMPLETE
Uploaded JAR:
  s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
S3 listing:
  2026-05-24 19:00:43 53147 emr-migration-demo-0.1.0.jar
Medium rerun submit: STARTED as full 3-step sequence
  Step 1 FeatureLogConverter:
    step id: s-068154131WPULWDW4XNP
    application id: application_1779641349593_0001
    state: COMPLETED
    start time: 2026-05-24T19:02:43.480000+00:00
    end time: 2026-05-24T19:03:31.597000+00:00
    runtime: 48 seconds
  Step 2 EligibleUserDataLogConverter:
    step id: s-00846262IMCXFAJXETUF
    application id: application_1779641349593_0002
    state: COMPLETED
    start time: 2026-05-24T19:03:36.602000+00:00
    end time: 2026-05-24T19:04:12.651000+00:00
    runtime: 36 seconds
  Step 3 BrbfJob disk baseline:
    step id: s-08787702FEIHIIC8N85H
    application id: application_1779641349593_0003
    state: RUNNING
    start time: 2026-05-24T19:04:17.656000+00:00
    Spark app start time: 2026-05-24T19:04:20.222GMT
    Spark History Server app state when checked: running / completed=false
  Live Spark UI checkpoint:
    active job: Job 37
    description: count at BrbfJob.scala:80
    submitted: 2026-05-24T19:06:16.341GMT
    duration when user copied UI: about 6.8 min
    stages: 0/7 succeeded
    tasks when user copied UI: 83/512, 13 running
    API snapshot shortly after: 96/512 completed, 13 active, 0 failed
    stage ids: 60, 56, 57, 61, 58, 55, 59
    follow-up API snapshot: 109/512 completed, 13 active, 0 failed
  Stage view checkpoint:
    active stage: Stage 61
    description: count at BrbfJob.scala:80
    duration when copied: 8.3 min
    tasks when copied: 109/196, 13 running
    input shown in UI: 2010.9 MiB
    shuffle write shown in UI: 117.2 GiB
    pending stages: 60, 59, 58, 57, 56, 55
  Stage 61 API checkpoint:
    status: ACTIVE
    tasks: 128/196 complete, 13 active, 0 failed
    memory spill: about 3.2 TiB
    disk spill: about 140 GiB
    peak execution memory: about 485 GiB aggregate
    shuffle read: about 2.3 GiB / 8,435,363 records
    shuffle write: about 141 GiB / 3,575,298,561 records
    GC time: about 55 s
    shuffle fetch wait: about 11 s
  Live Executors checkpoint:
    active executors including driver: 7
    dead executors: 0
    storage memory: 0.0 B / 59.3 GiB
    disk used: 0.0 B
    cores: 12
    active tasks: 13
    failed tasks: 0
    complete tasks: 566
    total tasks: 579
    task time: 1.6 h
    GC time: 55 s
    input: 1.3 GiB
    shuffle read: 8.3 GiB
    shuffle write: 117.8 GiB
    interpretation: heavy shuffle write and substantial task spill are visible,
      but no failed tasks and no dead executors are visible in this snapshot
  Job 37 completion checkpoint:
    completion time: 2026-05-24T19:21:48.775GMT
    status: SUCCEEDED
    completed tasks: 196
    skipped tasks: 316
    failed task attempts: 2
    killed tasks: 0
    failed stages: 0
    interpretation: the joined.count() path recovered from task retries and
      completed; Step 3 continued into later work
  Final Step 3 outcome:
    state: CANCELLED
    state change reason: Cancelled by user
    end time: 2026-05-24T20:46:26.039000+00:00
    reason: full medium was intentionally stopped after proving the larger-disk
      cluster could pass the first old failure checkpoint, but Job 38 showed
      elevated shuffle fetch instability and the run no longer matched the
      20-minute demo target
    final evidence before cancel:
      Job 37 succeeded
      Job 38 was running with 19 failed task attempts and 2 failed stage attempts
      failure evidence included FetchFailedException / No route to host
    next: create light-medium dataset and rerun with the same cluster/config
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

Do not change more than one major variable in the next rerun without recording the reason.
The intended controlled change is:

```text
larger worker local/EBS disk from cluster creation
same prepared medium data
same converted Step 1 and Step 2 outputs
Step 3 only
submit-time runtime overrides listed above
```
