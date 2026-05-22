# Databricks Post-Migration Plan

## Purpose

This document describes what to do when building the post-migration Databricks version of the EMR baseline pipeline.

The goal is to create a clean before/after story:

```text
Pre-migration:
  Airflow -> EMR steps -> EMR Spark JAR

Post-migration:
  Airflow -> Databricks Jobs -> lifted or optimized Databricks workload
```

## Recommended Migration Tracks

Use two phases instead of changing everything at once.

```text
Track A: Lift-and-run the existing JAR on Databricks
Track B: Refactor/optimize into Databricks-native jobs, notebooks, or a new optimized JAR
```

## Track A: Lift Existing JAR To Databricks

### Goal

Prove the same Scala Spark logic can run on Databricks with minimal changes.

This creates a fair first comparison:

```text
same input data
same business logic
same step order
different compute engine
```

### Inputs From The Pre-Migration EMR DAG

From the EMR Airflow DAG, capture:

```text
JAR path
main class names
runtime parameters
input paths
output paths
step order
validation checks
row counts
runtime per step
failure handling
```

Current EMR main classes:

```text
com.demo.emr.FeatureLogConverter
com.demo.emr.EligibleUserDataLogConverter
com.demo.emr.BrbfJob
```

Current expected JAR location after Step 11:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

Current runtime arguments:

```text
--bucket aigithub-emr-2026
--base-prefix emr-migration-demo
--run-date 2026-05-21
--hour 10
--late-hour 11
--output-mode overwrite
```

### Databricks Job Shape

Create a Databricks Job with three dependent tasks:

```text
Task 1: FeatureLogConverter
Task 2: EligibleUserDataLogConverter
Task 3: BrbfJob
```

Task dependencies:

```text
FeatureLogConverter
  -> EligibleUserDataLogConverter
    -> BrbfJob
```

Each task can run the same JAR with a different main class.

### JAR Placement Options

The JAR can be made available through:

```text
S3 path
DBFS
Unity Catalog volume
workspace files
```

Recommended first approach:

```text
Use the same S3 artifact path if Databricks has S3 read access.
```

Example:

```text
s3://aigithub-emr-2026/emr-migration-demo/artifacts/jars/emr-migration-demo-0.1.0.jar
```

### Output Path Separation

Do not overwrite EMR baseline outputs.

Current EMR output:

```text
s3://aigithub-emr-2026/emr-migration-demo/final/brbf/year=2026/month=05/day=21/hour=10/
```

Recommended Databricks output:

```text
s3://aigithub-emr-2026/emr-migration-demo/databricks/final/brbf/year=2026/month=05/day=21/hour=10/
```

This may require adding a future argument such as:

```text
--output-prefix databricks
```

or updating path-building logic to support an engine-specific output root.

### Track A Validation

Validate:

```text
converted feature_log count
converted eligible_user_data count
final brbf count
final business key uniqueness
important null rates
high/low frequency counts
late feedback count
```

Compare against EMR baseline values for the same run date/hour.

Tiny EMR baseline values to compare:

```text
feature_log converted count=50000
eligible_user_data converted count=20000
brbf joined row count=1080000
brbf high_frequency_joined_count=1000000
brbf low_frequency_joined_count=80000
brbf final_output_count=91883
```

## Track B: Databricks-Native Refactor

### Goal

Create an optimized Databricks implementation that is easier to inspect, tune, and compare against the EMR baseline.

Possible implementations:

```text
Databricks notebooks
new optimized Scala JAR
Databricks Asset Bundle
Delta Live Tables, if the story later calls for it
```

### Notebook Option

Create notebooks:

```text
notebooks/01_feature_log_converter
notebooks/02_eligible_user_data_converter
notebooks/03_brbf_job
```

In each notebook:

```text
copy the core transformation logic from the Scala class
replace command-line args with widgets
read the same input data
write to Databricks-specific output paths
add validation cells
record counts and metrics
```

Example widgets:

```scala
dbutils.widgets.text("bucket", "aigithub-emr-2026")
dbutils.widgets.text("base_prefix", "emr-migration-demo")
dbutils.widgets.text("run_date", "2026-05-21")
dbutils.widgets.text("hour", "10")
dbutils.widgets.text("late_hour", "11")
dbutils.widgets.text("output_mode", "overwrite")
```

### Optimized JAR Option

Create new optimized classes:

```text
com.demo.databricks.FeatureLogConverterOptimized
com.demo.databricks.EligibleUserDataLogConverterOptimized
com.demo.databricks.BrbfJobOptimized
```

This keeps the optimized implementation side by side with the EMR baseline implementation.

Recommended if:

```text
we want production-style packaging
we want unit/integration tests
we want Airflow to submit JAR tasks consistently
we want cleaner source control than notebook-only logic
```

### Databricks Asset Bundle Option

Eventually, package jobs as a Databricks Asset Bundle:

```text
databricks.yml
resources/jobs/*.yml
src/
notebooks/
```

Recommended later, after the first Databricks comparison works.

## Optimization Targets

When refactoring for Databricks, look for these EMR baseline patterns:

```text
UDFs
manual repartition
manual salting
early derived columns
Parquet writes
wide joins
sort/union pressure
```

### UDFs To Native Expressions

Baseline:

```text
Udfs.sha256String
Udfs.stableUuidFromString
Udfs.roundTimestampToMinutes
```

Possible Databricks improvements:

```text
sha2 native function
date_trunc/window-style timestamp functions
native SQL expressions
avoid UUID generation if not needed for joins
```

### AQE And Skew Handling

Baseline:

```text
AQE disabled
manual high/low split
manual salting
manual repartitioning
```

Possible Databricks improvements:

```text
enable AQE
enable skew join handling
reduce manual salting
compare physical plans
compare task skew
```

### Delta Lake Layout

Baseline:

```text
Parquet on S3
Snappy compression
manual partition paths
```

Possible Databricks improvements:

```text
Delta tables
table statistics
optimized writes
auto compaction if available
ZSTD or recommended compression strategy
liquid clustering or Z-order depending on table design
```

### Photon

Operators likely to benefit:

```text
scans
filters
projections
hash joins
aggregations
sorts
```

Operators less likely to benefit:

```text
opaque custom Scala UDF logic
manual skew code that forces extra shuffles
```

Detailed Photon migration guidance lives here:

```text
KT/databricks_photon_best_practices.md
```

## Airflow Post-Migration DAG

The post-migration Airflow DAG should mirror the EMR DAG logically.

Recommended DAG file:

```text
airflow/dags/emr_migration_demo_brbf_databricks_dag.py
```

Recommended operators:

```text
DatabricksSubmitRunOperator
DatabricksRunNowOperator
PythonOperator for validation and comparison
```

Provider package:

```bash
pip install apache-airflow-providers-databricks
```

Required Airflow connection:

```text
databricks_default
```

The connection should use:

```text
Databricks workspace host
token or service principal credentials
```

## Comparison Outputs

Write comparison metadata to a stable location:

```text
s3://aigithub-emr-2026/emr-migration-demo/validation/pipeline_comparison/run_date=YYYY-MM-DD/hour=HH/
```

Recommended fields:

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

Example pipeline types:

```text
emr_baseline
databricks_lifted_jar
databricks_optimized
```

## Recommended Execution Order

Use this order:

```text
1. Finish EMR Step 11.
2. Build Airflow pre-migration EMR DAG.
3. Lift same JAR into Databricks job.
4. Build Airflow post-migration Databricks DAG.
5. Validate same counts.
6. Create optimized Databricks implementation.
7. Compare EMR baseline vs Databricks lifted JAR.
8. Compare EMR baseline vs Databricks optimized implementation.
```

## Key Principle

Do not optimize too early.

First prove:

```text
same input
same logic
same counts
different compute engine
```

Then optimize:

```text
native expressions
AQE
Photon-friendly operations
Delta layout
less manual salting
better compression/layout
```

This gives the migration story a clean and credible progression.
