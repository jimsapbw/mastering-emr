# EMR Migration Demo Master Runbook

## Purpose

This is the living runbook for the EMR-side BRBF migration demo.

It documents:

- what we are building
- why we are building it this way
- what has been implemented
- how to run each step
- how to validate each step
- how to resume after a break
- how to capture baseline evidence for a later Databricks comparison

Update this document as each new step is completed.

## Demo Objective

Build an EMR Spark Scala workload that simulates a BRBF-style ad-tech pipeline reading multiple S3-backed Parquet datasets.

The EMR baseline intentionally includes:

- UDF-derived columns
- early column expansion
- manual repartitioning
- skewed users and hot keys
- high/low frequency branches
- manual salting in the main BRBF job
- union and sort pressure
- Parquet-on-S3 output

The target baseline should eventually run around 7-8 minutes on a small EMR cluster after data volume is tuned.

Databricks migration planning is deferred until the EMR baseline is implemented and measured.

## Current Status

Completed:

```text
Step 01: Create S3 prefixes
Step 02: Generate mock data
Step 03: Maven setup
Step 04: Dataset contract and validation
Step 05: GitHub/S3 backup process
Step 06: Scala Maven Spark project
Step 07: Shared Scala utilities
Step 08: FeatureLogConverter
Step 09: EligibleUserDataLogConverter
Step 10: BrbfJob
```

Next:

```text
Step 11: Package, upload JAR, and run EMR steps
```

## Repository And Backup Locations

GitHub:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Latest pushed commit at the time this runbook was created:

```text
db5768a Add EMR converter steps and KT docs
```

S3 code backup:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/
```

Local working folder:

```text
/home/hadoop/emr_migration_demo
```

Temporary Git clone used for the latest GitHub push:

```text
/mnt/tmp/mastering-emr-codex-backup
```

Local patch backup:

```text
emr_migration_demo/KT/0001-Add-EMR-converter-steps-and-KT-docs.patch
```

## S3 Layout

Bucket:

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

Current tiny run partition:

```text
year=2026/month=05/day=21/hour=10
```

Late feedback hour:

```text
year=2026/month=05/day=21/hour=11
```

Raw source prefixes:

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

Converted/final prefixes:

```text
converted/feature_log/
converted/eligible_user_data/
final/brbf/
validation/
artifacts/jars/
artifacts/configs/
artifacts/logs/
artifacts/code-backup/
```

## Dataset Design

The demo uses 8 raw datasets:

```text
bids
impressions_feedback
contextual
matched_user_data
advertiser
koa_settings
feature_log
sib
```

Key planned joins:

```text
bids.bid_request_id            -> impressions_feedback.bid_request_id
bids.bid_request_id            -> matched_user_data.bid_request_id
bids.advertiser_id             -> advertiser.advertiser_id
bids.advertiser_id/campaign_id -> koa_settings.advertiser_id/campaign_id
bids.contextual_id             -> contextual.contextual_id
bids.user_id_hash              -> feature_log.user_id_hash
bids.user_id_hash              -> sib.user_id_hash
```

Reference:

```text
KT/04_dataset_contract_and_validation.md
```

## Step 01: Create S3 Prefixes

Purpose:

```text
Create the raw, converted, final, validation, and artifact prefixes in S3.
```

Script:

```text
scripts/create_s3_prefixes.sh
```

Command:

```bash
bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Reference:

```text
KT/01_create_s3_prefixes.md
```

## Step 02: Generate Mock Data

Purpose:

```text
Generate S3-backed Snappy Parquet mock datasets with realistic skew and late feedback.
```

Script:

```text
scripts/generate_mock_data.py
```

Tiny command:

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

Tiny row counts:

```text
bids: 100,000
impressions_feedback: 70,000
contextual: 20,000
matched_user_data: 20,000
advertiser: 1,000
koa_settings: 1,000
feature_log: 50,000
sib: 20,000
```

Reference:

```text
KT/02_mock_dataset_generation.md
```

## Step 03: Maven Setup

Purpose:

```text
Install Maven so the Scala Spark project can be compiled and packaged into a JAR.
```

Command:

```bash
sudo dnf install -y maven
```

Builds may need a writable local Maven repo:

```bash
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Reference:

```text
KT/03_maven_setup.md
```

## Step 04: Dataset Contract And Validation

Purpose:

```text
Validate row counts, join-key overlap, late feedback, and skew indicators.
```

Script:

```text
scripts/validate_mock_data.py
```

Command:

```bash
spark-submit \
  --master local[*] \
  emr_migration_demo/scripts/validate_mock_data.py \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Observed tiny validation:

```text
counts.bids=100000
counts.impressions_feedback_total=70000
counts.contextual=20000
counts.matched_user_data=20000
counts.advertiser=1000
counts.koa_settings=1000
counts.feature_log=50000
counts.sib=20000
late_feedback.same_hour_rows=59500
late_feedback.late_n_plus_1_rows=10500
```

Reference:

```text
KT/04_dataset_contract_and_validation.md
```

## Step 05: Backup Process

Purpose:

```text
Keep the demo recoverable through GitHub and S3 backup paths.
```

S3 backup command:

```bash
aws s3 sync emr_migration_demo s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ --region us-east-1
```

S3 restore command:

```bash
aws s3 sync s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ emr_migration_demo/ --region us-east-1
```

GitHub backup:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Reference:

```text
KT/05_git_backup.md
KT/00_resume_plan.md
```

## Step 06: Scala Maven Spark Project

Purpose:

```text
Create a Scala/Maven Spark project that builds one reusable JAR.
```

Key files:

```text
spark/pom.xml
spark/src/main/scala/com/demo/emr/RawDataCountApp.scala
```

Build command:

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Raw count command:

```bash
spark-submit \
  --master local[*] \
  --class com.demo.emr.RawDataCountApp \
  emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11
```

Reference:

```text
KT/06_scala_maven_spark_project.md
```

## Step 07: Shared Scala Utilities

Purpose:

```text
Add reusable helpers so the pipeline entry points stay focused on business logic.
```

Files:

```text
common/AppConfig.scala
common/ArgsParser.scala
common/S3Paths.scala
common/SparkSessionFactory.scala
common/DatasetIO.scala
common/Udfs.scala
```

Responsibilities:

```text
argument parsing
Spark session creation
S3 path building
Parquet read/write helpers
shared UDFs
```

Reference:

```text
KT/07_shared_scala_utilities.md
```

## Step 08: FeatureLogConverter

Purpose:

```text
Convert raw feature_log into converted/feature_log for downstream BRBF joins.
```

Entry point:

```text
com.demo.emr.FeatureLogConverter
```

Source:

```text
raw/feature_log/year=2026/month=05/day=21/hour=10/
```

Output:

```text
converted/feature_log/year=2026/month=05/day=21/hour=10/
```

Implemented behavior:

```text
read raw feature_log
normalize user id
preserve source hash
recompute user hash
create stable UUID
create contextual join key
round event timestamp
bucket feature value
deduplicate by feature_event_id
repartition by user_id_hash and contextual_id
write converted feature_log
```

Run command:

```bash
spark-submit \
  --master local[*] \
  --class com.demo.emr.FeatureLogConverter \
  emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --output-mode overwrite
```

Observed validation:

```text
feature_log_converter.raw_feature_log_count=50000
feature_log_converter.converted_feature_log_count=50000
feature_log_converter.distinct_user_hash_count=40200
feature_log_converter.distinct_contextual_count=13035
```

Reference:

```text
KT/08_feature_log_converter.md
```

## Step 09: EligibleUserDataLogConverter

Purpose:

```text
Convert raw matched_user_data into converted/eligible_user_data for downstream BRBF joins.
```

Entry point:

```text
com.demo.emr.EligibleUserDataLogConverter
```

Source:

```text
raw/matched_user_data/year=2026/month=05/day=21/hour=10/
```

Output:

```text
converted/eligible_user_data/year=2026/month=05/day=21/hour=10/
```

Implemented behavior:

```text
read raw matched_user_data
normalize user id
preserve source hash
recompute user hash
create stable UUID
create bid request hash
derive high/low frequency bracket
derive eligibility band
create eligible_user_flag
deduplicate by bid_request_id and user_id_hash
repartition by user_id_hash and frequency_bracket
write converted eligible_user_data
```

Run command:

```bash
spark-submit \
  --master local[*] \
  --class com.demo.emr.EligibleUserDataLogConverter \
  emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --output-mode overwrite
```

Observed validation:

```text
eligible_user_data_converter.raw_matched_user_data_count=20000
eligible_user_data_converter.converted_eligible_user_data_count=20000
eligible_user_data_converter.eligible_user_count=5000
eligible_user_data_converter.high_frequency_count=4000
eligible_user_data_converter.low_frequency_count=16000
```

Reference:

```text
KT/09_eligible_user_data_converter.md
```

## Step 10: BrbfJob

Status:

```text
Implemented and tiny smoke tested.
```

Purpose:

```text
Implement the main BRBF before-DAG workload.
```

Entry point:

```text
com.demo.emr.BrbfJob
```

Implemented flow:

```text
read bids
read impressions same-hour and late-hour
read contextual
read advertiser
read koa_settings
read converted feature_log
read converted eligible_user_data
read sib
compute derived bid columns early
join impressions
join contextual
broadcast join advertiser
broadcast join koa_settings
join eligible user data
join feature log
join sib
split high/low frequency users
apply manual salting for high-frequency branch
aggregate high/low branches
union branches
sort
write final/brbf
```

Expected baseline symptoms:

```text
wide rows before joins
large shuffle stages
manual salting complexity
skewed task durations
sort/union pressure
Parquet-on-S3 output
```

Run command:

```bash
spark-submit \
  --master local[*] \
  --class com.demo.emr.BrbfJob \
  emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --output-mode overwrite
```

Observed tiny validation:

```text
brbf_job.source_bids_count=100000
brbf_job.source_impressions_count=70000
brbf_job.source_contextual_count=20000
brbf_job.source_advertiser_count=1000
brbf_job.source_koa_settings_count=1000
brbf_job.source_eligible_user_data_count=20000
brbf_job.source_feature_log_count=50000
brbf_job.source_sib_count=16200
brbf_job.joined_row_count=1080000
brbf_job.high_frequency_joined_count=1000000
brbf_job.low_frequency_joined_count=80000
brbf_job.final_output_count=91883
```

Reference:

```text
KT/10_brbf_job.md
```

## Databricks Migration Planning

Purpose:

```text
Keep the post-migration planning separate from the EMR baseline implementation,
while still linking the migration story back to this runbook.
```

References:

```text
KT/databricks_post_migration_plan.md
KT/databricks_photon_best_practices.md
KT/airflow_orchestration_plan.md
KT/future_migration_stories.md
```

## Step 11: Package, Upload JAR, And Run EMR Steps

Status:

```text
Not implemented yet.
```

Purpose:

```text
Run the full three-step pipeline as EMR steps.
```

Planned steps:

```text
FeatureLogConverter
EligibleUserDataLogConverter
BrbfJob
```

JAR upload target:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

## Future Airflow Orchestration

After the EMR steps are manually validated, Airflow can orchestrate the pipeline.

The future orchestration plan includes two Airflow DAGs:

```text
Pre-migration DAG: EMR baseline pipeline
Post-migration DAG: Databricks pipeline
```

Planned Airflow task flow:

```text
validate raw partitions
submit FeatureLogConverter EMR step
wait for FeatureLogConverter
validate converted feature_log
submit EligibleUserDataLogConverter EMR step
wait for EligibleUserDataLogConverter
validate converted eligible_user_data
submit BrbfJob EMR step
wait for BrbfJob
validate final/brbf output
capture metrics
```

Reference:

```text
KT/airflow_orchestration_plan.md
```

## Step 12: Scale Data And Tune Runtime

Status:

```text
Not started.
```

Purpose:

```text
Scale mock data until the EMR baseline approaches the target runtime.
```

Start with:

```text
small scale
```

Then tune toward:

```text
7-8 minute runtime on the chosen EMR cluster
```

## Troubleshooting And Evidence Capture

Use Spark UI or Spark History Server to capture:

```text
runtime
slowest jobs
slowest stages
shuffle read/write
memory spill
disk spill
task duration variance
join strategies
physical operators
```

Reference:

```text
KT/emr_spark_troubleshooting_guide.md
```

## Baseline Problem Statement

The EMR baseline intentionally introduces:

```text
UDF-heavy transformations
early derived-column creation
manual repartitioning
skew setup
Parquet-on-S3 writes
```

Reference:

```text
KT/emr_baseline_problem_statement.md
```

## Future Migration Stories

Future optimization stories are tracked separately so they do not destabilize the first EMR baseline.

Current future stories include:

```text
legacy compression to ZSTD
UDFs to native Spark SQL expressions
AQE and skew join handling
manual salting removal
Parquet to Delta Lake
Photon acceleration
```

Reference:

```text
KT/future_migration_stories.md
```

## Databricks Post-Migration Plan

The future post-migration implementation should first lift the existing JAR into Databricks, then create an optimized Databricks-native version.

Reference:

```text
KT/databricks_post_migration_plan.md
```

## Resume Instructions

To resume:

```text
Read KT/00_resume_plan.md first.
Then read this master runbook.
Continue from the first incomplete step.
```

Current next step:

```text
Step 11: Package, upload JAR, and run EMR steps
```
