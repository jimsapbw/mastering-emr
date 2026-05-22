# Resume Plan

Use this file to resume the EMR-side migration demo after a break, cluster termination, or new Codex session.

## Current Goal

Build an EMR-side Spark demo that reproduces the BRBF-style **before DAG**:

```text
Read sources
Compute columns early
Impression join
Advertiser join
Feature-log join
SIB join
Manual salting
Union + sort
Output to S3
```

Databricks migration planning is intentionally deferred until the EMR baseline is built and measured.

Master runbook:

```text
emr_migration_demo/KT/emr_migration_demo_master_runbook.md
```

## Completed So Far

### 1. S3 Prefixes

Created the demo layout under:

```text
s3://aigithub-emr-2026/emr-migration-demo/
```

Main folders:

```text
raw/
converted/
final/
validation/
artifacts/
```

Raw datasets:

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

Reference:

```text
emr_migration_demo/KT/01_create_s3_prefixes.md
```

### 2. Mock Data Generator

Created:

```text
emr_migration_demo/scripts/generate_mock_data.py
```

Ran the `tiny` scale preset and wrote Snappy Parquet to S3.

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
emr_migration_demo/KT/02_mock_dataset_generation.md
```

### 3. Maven Setup

Installed Maven:

```text
Apache Maven 3.8.4
```

Reference:

```text
emr_migration_demo/KT/03_maven_setup.md
```

### 4. Dataset Contract And Validation

Created:

```text
emr_migration_demo/scripts/validate_mock_data.py
emr_migration_demo/KT/04_dataset_contract_and_validation.md
```

Validation passed on the tiny dataset.

Important observed results:

```text
counts.bids=100000
counts.impressions_feedback_total=70000
late_feedback.same_hour_rows=59500
late_feedback.late_n_plus_1_rows=10500
```

All planned joins have non-zero key overlap.

Reference:

```text
emr_migration_demo/KT/04_dataset_contract_and_validation.md
```

### 5. GitHub Backup

Backed up the demo scaffold to:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Commit:

```text
eed4b8c Add EMR migration demo scaffold
```

Reference:

```text
emr_migration_demo/KT/05_git_backup.md
```

Latest GitHub backup:

```text
Repository: https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
Branch: main
Commit: 900df9d Add BRBF job and migration KT docs
```

This latest commit includes:

```text
Step 08 FeatureLogConverter
Step 09 EligibleUserDataLogConverter
Step 10 BrbfJob
KT docs for steps 08 and 09
KT docs for Step 10 implementation and validation
EMR baseline problem statement
EMR Spark troubleshooting guide
Master runbook
Airflow orchestration plan
Databricks post-migration plan
Databricks Photon best practices
Future migration stories
Updated resume plan and KT index
```

The commit was created from:

```text
/mnt/tmp/mastering-emr-codex-backup
```

and pushed with GitHub HTTPS/PAT authentication.

Local patch backup:

```text
emr_migration_demo/KT/0001-Add-EMR-converter-steps-and-KT-docs.patch
```

### 6. Scala Maven Spark Project

Created the Scala/Maven project:

```text
emr_migration_demo/spark/pom.xml
emr_migration_demo/spark/src/main/scala/com/demo/emr/RawDataCountApp.scala
```

Verified:

```text
Maven build: PASS
RawDataCountApp S3 read/count run: PASS
```

Reference:

```text
emr_migration_demo/KT/06_scala_maven_spark_project.md
```

### 7. Shared Scala Utilities

Created shared utilities:

```text
common/AppConfig.scala
common/ArgsParser.scala
common/S3Paths.scala
common/SparkSessionFactory.scala
common/DatasetIO.scala
common/Udfs.scala
```

Refactored `RawDataCountApp` to use the helpers and verified the same tiny S3 counts.

Reference:

```text
emr_migration_demo/KT/07_shared_scala_utilities.md
```

### 8. Feature Log Converter

Created the first EMR pipeline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
```

Verified:

```text
Maven build: PASS
FeatureLogConverter tiny S3 run: PASS
```

Observed:

```text
feature_log_converter.raw_feature_log_count=50000
feature_log_converter.converted_feature_log_count=50000
feature_log_converter.distinct_user_hash_count=40200
feature_log_converter.distinct_contextual_count=13035
```

Reference:

```text
emr_migration_demo/KT/08_feature_log_converter.md
```

### 9. Eligible User Data Converter

Created the second EMR pipeline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/EligibleUserDataLogConverter.scala
```

Verified:

```text
Maven build: PASS
EligibleUserDataLogConverter tiny S3 run: PASS
```

Observed:

```text
eligible_user_data_converter.raw_matched_user_data_count=20000
eligible_user_data_converter.converted_eligible_user_data_count=20000
eligible_user_data_converter.eligible_user_count=5000
eligible_user_data_converter.high_frequency_count=4000
eligible_user_data_converter.low_frequency_count=16000
```

Reference:

```text
emr_migration_demo/KT/09_eligible_user_data_converter.md
```

### 10. BRBF Job

Created the main EMR baseline entry point:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/BrbfJob.scala
```

Verified:

```text
Maven build: PASS
BrbfJob tiny S3 run: PASS
```

Observed:

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
emr_migration_demo/KT/10_brbf_job.md
emr_migration_demo/KT/10_brbf_job_validation.md
```

### 11. Step 10 Manual Validation Progress

Created:

```text
emr_migration_demo/KT/10_brbf_job_validation.md
```

Validated manually in `spark-shell`:

```text
raw bids read correctly
prepareBids created early derived columns
supporting datasets were prepared
sample bids joined to impressions
sample bids joined to contextual
sample bids joined to advertiser
sample bids joined to KOA settings
sample bids joined to eligible_user_data
sample bids joined to feature_log
sample bids joined to SIB
5 sample bids expanded to 5000 joined rows
post-join columns were created
high/low aggregation example was documented
```

Current pause point:

```text
Resume with KT/10_brbf_job_validation.md.
Continue from Validation 7: Final Output.
Then proceed to Step 11: package/upload JAR and run EMR steps.
```

## Important Commands

Create S3 prefixes:

```bash
bash emr_migration_demo/scripts/create_s3_prefixes.sh aigithub-emr-2026
```

Generate tiny mock data:

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

Validate mock data:

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

Back up local demo files to S3:

```bash
aws s3 sync emr_migration_demo s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ --region us-east-1
```

Restore local demo files from S3:

```bash
aws s3 sync s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/ emr_migration_demo/ --region us-east-1
```

S3 code backup location:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/code-backup/emr_migration_demo/
```

GitHub code backup location:

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

Install Maven on a new cluster:

```bash
sudo dnf install -y maven
```

## Next Recommended Steps

### Step 11: Package And Run Tiny End-To-End

Build:

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Upload JAR:

```bash
aws s3 cp target/*.jar s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/ --region us-east-1
```

Run the 3 steps on tiny data as EMR steps and validate output.

### Step 12: Scale Data

After the tiny end-to-end pipeline works, regenerate data with `small`:

```text
bids: 1,000,000
impressions_feedback: 700,000
feature_log: 500,000
contextual: 200,000
matched_user_data: 200,000
sib: 100,000
advertiser: 10,000
koa_settings: 10,000
```

Only scale further after the EMR baseline works correctly.

## Resume Prompt

Use this prompt in a future Codex session:

```text
Read emr_migration_demo/KT/00_resume_plan.md and KT/10_brbf_job_validation.md. Continue from Step 10 Validation 7 final output checks, then proceed to Step 11: package the JAR, upload it to S3, and run the three EMR steps.
```
