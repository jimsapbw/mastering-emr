# Airflow Orchestration Plan

## Purpose

This document describes how the EMR migration demo can eventually be orchestrated from an Airflow DAG.

The current EMR baseline is being developed and tested with manual `spark-submit` first. Once the three Spark entry points are stable and the JAR is uploaded to S3, Airflow can submit and monitor the EMR steps.

## Current Pipeline Steps

The Spark pipeline has three application entry points:

```text
com.demo.emr.FeatureLogConverter
com.demo.emr.EligibleUserDataLogConverter
com.demo.emr.BrbfJob
```

Execution order:

```text
FeatureLogConverter
  -> EligibleUserDataLogConverter
    -> BrbfJob
```

## Airflow Orchestration Goal

We eventually want two Airflow pipelines:

```text
1. Pre-migration Airflow DAG
   Orchestrates the EMR baseline.

2. Post-migration Airflow DAG
   Orchestrates the Databricks version.
```

The first DAG proves and measures the current EMR-style implementation.

The second DAG runs the migrated Databricks implementation so we can compare runtime, cost, output counts, and operational behavior.

## Pipeline 1: Pre-Migration EMR DAG

Airflow should:

```text
accept run_date and hour parameters
verify required raw partitions exist
submit FeatureLogConverter as an EMR step
wait for FeatureLogConverter completion
submit EligibleUserDataLogConverter as an EMR step
wait for EligibleUserDataLogConverter completion
submit BrbfJob as an EMR step
wait for BrbfJob completion
run validation checks
record metrics/log locations
notify on success or failure
```

This DAG runs:

```text
EMR cluster
EMR steps
Spark JAR from S3
Parquet input/output on S3
```

Current Spark entry points:

```text
com.demo.emr.FeatureLogConverter
com.demo.emr.EligibleUserDataLogConverter
com.demo.emr.BrbfJob
```

## Pipeline 2: Post-Migration Databricks DAG

The post-migration DAG should run the Databricks version of the same logical pipeline.

Detailed Databricks migration planning lives in:

```text
KT/databricks_post_migration_plan.md
```

There are two possible approaches:

```text
Option A: Run the same Scala JAR on Databricks Jobs
Option B: Refactor/migrate the logic into Databricks notebooks or Databricks Asset Bundles
```

For this demo, the initial post-migration story can use:

```text
Airflow -> Databricks Jobs API -> Spark job/task
```

The Databricks DAG should:

```text
accept the same run_date and hour parameters
verify required input partitions or tables exist
submit FeatureLogConverter equivalent
submit EligibleUserDataLogConverter equivalent
submit BrbfJob equivalent
run validation checks
capture Databricks job run IDs
capture Spark UI / query profile links
capture output counts and runtime
compare results against EMR baseline
```

Databricks execution options:

```text
DatabricksSubmitRunOperator
DatabricksRunNowOperator
Databricks Jobs API
Databricks Asset Bundles
```

If running the JAR directly from Databricks, the JAR can be stored in:

```text
S3 artifact path
DBFS
Unity Catalog volume
workspace files
```

Possible JAR location:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

Databricks job tasks would map to:

```text
FeatureLogConverter task
EligibleUserDataLogConverter task
BrbfJob task
```

For a more complete Databricks migration, the classes may eventually change to optimized versions:

```text
com.demo.databricks.FeatureLogConverterOptimized
com.demo.databricks.EligibleUserDataLogConverterOptimized
com.demo.databricks.BrbfJobOptimized
```

or notebook tasks:

```text
notebooks/feature_log_converter
notebooks/eligible_user_data_converter
notebooks/brbf_job
```

## Pre/Post Comparison Goal

Both Airflow DAGs should produce comparable run metadata.

Compare:

```text
runtime by step
total pipeline runtime
input counts
output counts
join match rates
final business metrics
shuffle read/write
spill
skew symptoms
operator plan
cost estimate
failure/retry behavior
```

The output paths should be separated.

Example EMR output:

```text
s3://aigithub-emr-2026/emr-migration-demo/final/brbf/year=2026/month=05/day=21/hour=10/
```

Example Databricks output:

```text
s3://aigithub-emr-2026/emr-migration-demo/databricks/final/brbf/year=2026/month=05/day=21/hour=10/
```

or, if using Delta:

```text
s3://aigithub-emr-2026/emr-migration-demo/delta/brbf/
```

Recommended comparison table:

```text
validation/pipeline_comparison/run_date=YYYY-MM-DD/hour=HH/
```

Fields:

```text
run_date
hour
pipeline_type
orchestrator
compute_engine
step_name
runtime_seconds
input_count
output_count
status
application_or_run_id
output_path
```

## Prerequisites

Before building the Airflow DAG, complete:

```text
Step 10: BrbfJob implemented and smoke tested
Step 11: JAR uploaded to S3
Step 11: Three EMR steps manually tested
```

Before building the Databricks DAG, complete:

```text
Databricks workspace available
Databricks cluster or job cluster policy decided
Databricks token/service principal configured for Airflow
JAR accessible from Databricks
input S3 access configured
output path/table strategy decided
baseline EMR outputs available for comparison
```

Required S3 artifact:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

Required runtime parameters:

```text
bucket
base_prefix
run_date
hour
late_hour
output_mode
cluster_id
aws_region
```

Example defaults:

```text
bucket=aigithub-emr-2026
base_prefix=emr-migration-demo
run_date=2026-05-21
hour=10
late_hour=11
output_mode=overwrite
aws_region=us-east-1
```

## Recommended EMR DAG Shape

Recommended Airflow task graph:

```text
start
  |
validate_raw_partitions
  |
upload_or_verify_jar
  |
add_feature_log_converter_step
  |
watch_feature_log_converter_step
  |
validate_feature_log_converted
  |
add_eligible_user_data_converter_step
  |
watch_eligible_user_data_converter_step
  |
validate_eligible_user_data_converted
  |
add_brbf_job_step
  |
watch_brbf_job_step
  |
validate_brbf_output
  |
capture_metrics
  |
end
```

## Airflow Operators

This demo assumes self-managed/open-source Apache Airflow, not Amazon MWAA.

The DAG can still use the Airflow Amazon provider package to submit EMR steps.

Use:

```text
EmrAddStepsOperator
EmrStepSensor
S3KeySensor or custom S3 partition check
PythonOperator for validation/metrics
```

For the Databricks DAG, use the Databricks provider:

```text
DatabricksSubmitRunOperator
DatabricksRunNowOperator
PythonOperator for validation/metrics
```

Possible imports:

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
```

For self-managed Airflow, install or verify the Databricks provider:

```bash
pip install apache-airflow-providers-databricks
```

Possible imports:

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
```

Exact import paths can vary by Airflow AWS provider version.

Amazon MWAA is optional and not required for this demo.

For a self-managed Airflow environment, install or verify the AWS provider:

```bash
pip install apache-airflow-providers-amazon
```

The Airflow worker/scheduler environment also needs AWS credentials or an IAM role/profile that can:

```text
describe EMR clusters
add EMR steps
describe EMR steps
read/write required S3 paths
```

## EMR Step Args

### FeatureLogConverter

Spark submit args:

```text
spark-submit
--class
com.demo.emr.FeatureLogConverter
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
--bucket
aigithub-emr-2026
--base-prefix
emr-migration-demo
--run-date
2026-05-21
--hour
10
--late-hour
11
--output-mode
overwrite
```

### EligibleUserDataLogConverter

Spark submit args:

```text
spark-submit
--class
com.demo.emr.EligibleUserDataLogConverter
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
--bucket
aigithub-emr-2026
--base-prefix
emr-migration-demo
--run-date
2026-05-21
--hour
10
--late-hour
11
--output-mode
overwrite
```

### BrbfJob

Spark submit args:

```text
spark-submit
--class
com.demo.emr.BrbfJob
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
--bucket
aigithub-emr-2026
--base-prefix
emr-migration-demo
--run-date
2026-05-21
--hour
10
--late-hour
11
--output-mode
overwrite
```

## Example EMR Step Definition Shape

Airflow `EmrAddStepsOperator` step definitions generally look like:

```python
FEATURE_LOG_STEP = {
    "Name": "FeatureLogConverter",
    "ActionOnFailure": "CANCEL_AND_WAIT",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--class", "com.demo.emr.FeatureLogConverter",
            "s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar",
            "--bucket", "aigithub-emr-2026",
            "--base-prefix", "emr-migration-demo",
            "--run-date", "{{ ds }}",
            "--hour", "{{ params.hour }}",
            "--late-hour", "{{ params.late_hour }}",
            "--output-mode", "overwrite",
        ],
    },
}
```

For this demo, `{{ ds }}` can map to `run_date` if the Airflow DAG schedule matches the data date.

If the DAG is manually triggered, prefer explicit `dag_run.conf` values.

## Parameter Handling

Recommended Airflow parameters:

```python
params={
    "bucket": "aigithub-emr-2026",
    "base_prefix": "emr-migration-demo",
    "hour": "10",
    "late_hour": "11",
    "output_mode": "overwrite",
}
```

For manual runs, allow overrides:

```text
dag_run.conf["run_date"]
dag_run.conf["hour"]
dag_run.conf["late_hour"]
```

Example manual trigger payload:

```json
{
  "run_date": "2026-05-21",
  "hour": "10",
  "late_hour": "11",
  "output_mode": "overwrite"
}
```

## Partition Validation Tasks

Before submitting EMR steps, validate these raw paths exist:

```text
raw/bids/year=YYYY/month=MM/day=DD/hour=HH/
raw/impressions_feedback/year=YYYY/month=MM/day=DD/hour=HH/
raw/impressions_feedback/year=YYYY/month=MM/day=DD/hour=LATE_HH/
raw/contextual/year=YYYY/month=MM/day=DD/hour=HH/
raw/matched_user_data/year=YYYY/month=MM/day=DD/hour=HH/
raw/feature_log/year=YYYY/month=MM/day=DD/hour=HH/
raw/advertiser/year=YYYY/month=MM/day=DD/
raw/koa_settings/year=YYYY/month=MM/day=DD/
raw/sib/year=YYYY/month=MM/day=DD/
```

After converter steps, validate:

```text
converted/feature_log/year=YYYY/month=MM/day=DD/hour=HH/
converted/eligible_user_data/year=YYYY/month=MM/day=DD/hour=HH/
```

After BRBF:

```text
final/brbf/year=YYYY/month=MM/day=DD/hour=HH/
```

## Output Validation Tasks

Minimum Airflow validation checks:

```text
converted feature_log count > 0
converted eligible_user_data count > 0
final brbf count > 0
final output partition exists
```

Better validation checks:

```text
source row counts
converted row counts
joined match rates
high/low frequency counts
late feedback count
final output count
duplicate final key checks
null-rate checks for important columns
```

These can be implemented as:

```text
Spark validation app
PySpark validation script
Airflow PythonOperator using boto3/Athena/EMR step logs
```

## Metrics To Capture

For each DAG run, capture:

```text
dag_run_id
run_date
hour
cluster_id
EMR step IDs
EMR step durations
Spark application IDs
output paths
row counts
validation status
failure reason if any
```

For performance baseline runs, also capture:

```text
shuffle read/write
spill
slowest stage
task skew symptoms
Spark History Server URL
```

## Failure Handling

Recommended EMR step behavior:

```text
ActionOnFailure=CANCEL_AND_WAIT
```

This keeps the cluster alive for troubleshooting.

Airflow should fail fast if:

```text
required raw partition is missing
JAR is missing
EMR step fails
validation count is zero
final output partition is missing
```

## Future DAG File Location

Recommended future DAG path:

```text
airflow/dags/emr_migration_demo_brbf_emr_dag.py
airflow/dags/emr_migration_demo_brbf_databricks_dag.py
```

Optional support files:

```text
airflow/include/emr_steps.py
airflow/include/databricks_jobs.py
airflow/include/validation.py
airflow/include/comparison.py
airflow/README.md
```

For self-managed Airflow, this folder would be copied or mounted into the Airflow DAGs directory.

Example local Airflow DAGs location:

```text
~/airflow/dags/
```

Example Docker-based Airflow DAGs mount:

```text
./airflow/dags:/opt/airflow/dags
```

## Recommended Implementation Timing

Do this after Step 11 succeeds manually:

```text
1. Upload JAR to S3
2. Run all three steps manually with aws emr add-steps
3. Confirm final output and validation
4. Create pre-migration EMR Airflow DAG
5. Run EMR DAG manually for tiny partition
6. Add validation and metrics capture
7. Create post-migration Databricks Airflow DAG
8. Run Databricks DAG manually for same tiny partition
9. Add comparison validation
10. Scale data
```

## Relationship To Current Runbook

This Airflow plan is a future orchestration layer over the existing EMR baseline.

It should not change the Spark job logic.

The Spark jobs remain the source of business transformation logic:

```text
FeatureLogConverter
EligibleUserDataLogConverter
BrbfJob
```

Airflow should orchestrate, validate, and report status.
