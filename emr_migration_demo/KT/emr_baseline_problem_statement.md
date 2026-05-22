# EMR Baseline Problem Statement

## Purpose

This demo intentionally builds an EMR Spark Scala baseline that behaves like a realistic BRBF-style ad-tech workload before migration improvements.

The goal is not only to make the jobs work. The goal is to create a credible baseline with visible optimization opportunities that can later be improved and compared on Databricks.

For the troubleshooting workflow and Spark UI evidence to capture, see:

```text
emr_migration_demo/KT/emr_spark_troubleshooting_guide.md
```

For future migration improvement stories that are not part of the first EMR baseline, see:

```text
emr_migration_demo/KT/future_migration_stories.md
```

## Problem Statement

Build an EMR Spark Scala baseline that simulates a BRBF-style ad-tech pipeline. The baseline reads multiple S3-backed Parquet datasets, computes UDF-derived columns early, repartitions data manually, joins large and skewed datasets, handles high-frequency users through custom logic, and writes partitioned Parquet outputs.

This baseline is intentionally designed to show:

- UDF overhead
- early derived-column expansion
- manual repartitioning
- shuffle pressure
- skew-handling complexity
- regular Parquet-on-S3 file layout

Later, the Databricks version can improve or compare these areas using:

- native Spark SQL/DataFrame expressions
- Adaptive Query Execution
- skew-aware join handling
- better physical planning
- Delta Lake layout and metadata features
- less manual salting/repartitioning code

Short version:

```text
We are building a realistic but intentionally old-school EMR Spark baseline:
UDFs, early derived columns, manual repartitioning, skewed users, and Parquet on S3.

Later, Databricks can improve it with native expressions, AQE, optimized joins,
better layout, Delta, and less manual skew-handling code.
```

## Baseline Pain Points We Are Introducing

### 1. UDF-Heavy Transformations

The EMR baseline intentionally uses Scala UDFs for transformations such as:

```text
sha256String
stableUuidFromString
roundTimestampToMinutes
```

Why this matters:

```text
UDFs can hide logic from Spark's optimizer and add per-row execution overhead.
```

Later migration opportunity:

```text
Replace UDF-heavy logic with native Spark SQL/DataFrame functions where possible.
```

### 2. Early Derived-Column Creation

The baseline creates derived columns before the main BRBF job consumes the data.

Examples:

```text
normalized_user_id
user_id_hash
user_uuid
bid_request_hash
contextual_join_key
rounded_event_time
feature_value_bucket
eligibility_band
frequency_bracket
```

Why this matters:

```text
Adding columns early can increase row width before joins and shuffles.
```

Later migration opportunity:

```text
Defer nonessential derived columns until after filtering or joining when possible.
```

### 3. Manual Repartitioning

The baseline manually repartitions converted data before writing.

Why this matters:

```text
Manual repartitioning creates explicit shuffle stages and may not match the best runtime layout.
```

Later migration opportunity:

```text
Compare manual repartitioning against AQE, optimized writes, and better table layout.
```

### 4. Skew Setup

The mock data intentionally creates hot users and hot keys. The converters preserve and expose this through fields such as:

```text
frequency_bracket
```

Why this matters:

```text
The main BRBF job can split high-frequency and low-frequency users and demonstrate manual skew handling.
```

Later migration opportunity:

```text
Compare custom skew handling and manual salting against Databricks/Spark adaptive skew optimizations.
```

### 5. Parquet-On-S3 Baseline

The EMR baseline writes standard Snappy Parquet files to S3.

Why this matters:

```text
Regular Parquet on S3 gives us a simple baseline without transaction logs, table statistics, or Delta-specific layout features.
```

Later migration opportunity:

```text
Compare against Delta Lake features such as metadata statistics, optimized file layout, and clustering strategies.
```

## Step Mapping

### Step 08: FeatureLogConverter

Entry point:

```text
com.demo.emr.FeatureLogConverter
```

Introduced baseline traits:

- UDF-heavy transformations:

```text
user_id_hash         = Udfs.sha256String(normalized_user_id)
user_uuid            = Udfs.stableUuidFromString(normalized_user_id)
contextual_join_key  = Udfs.sha256String(contextual_id)
rounded_event_time   = Udfs.roundTimestampToMinutes(5)(event_timestamp)
```

- Early derived columns:

```text
normalized_user_id
source_user_id_hash
user_id_hash
user_uuid
contextual_join_key
rounded_event_time
feature_value_bucket
```

- Manual repartitioning:

```scala
converted.repartition($"user_id_hash", $"contextual_id")
```

Purpose in the baseline:

```text
Prepare feature-log join keys and time buckets before the main BRBF job, while creating visible UDF and shuffle behavior.
```

### Step 09: EligibleUserDataLogConverter

Entry point:

```text
com.demo.emr.EligibleUserDataLogConverter
```

Introduced baseline traits:

- UDF-heavy transformations:

```text
user_id_hash      = Udfs.sha256String(normalized_user_id)
user_uuid         = Udfs.stableUuidFromString(normalized_user_id)
bid_request_hash  = Udfs.sha256String(bid_request_id)
```

- Early derived columns:

```text
normalized_user_id
source_user_id_hash
user_id_hash
user_uuid
bid_request_hash
frequency_bracket
eligibility_band
eligible_user_flag
```

- Manual repartitioning:

```scala
converted.repartition($"user_id_hash", $"frequency_bracket")
```

- Skew setup:

```text
hot_user_* -> frequency_bracket = high
all others -> frequency_bracket = low
```

Purpose in the baseline:

```text
Prepare user eligibility and high/low frequency split data before the main BRBF job, while making manual skew-handling inputs explicit.
```

### Step 10: BrbfJob

Entry point:

```text
com.demo.emr.BrbfJob
```

Introduced baseline traits:

- AQE disabled for the before-DAG baseline:

```text
spark.sql.adaptive.enabled=false
spark.sql.adaptive.skewJoin.enabled=false
```

- Early bid-derived columns:

```text
normalized_user_id
source_user_id_hash
user_id_hash
bid_request_hash
contextual_join_key
rounded_bid_time
bid_price_bucket
event_quality_score
campaign_grouping_key
```

- UDF-heavy transformations in the critical path:

```text
user_id_hash         = Udfs.sha256String(normalized_user_id)
bid_request_hash     = Udfs.sha256String(bid_request_id)
contextual_join_key  = Udfs.sha256String(contextual_id)
rounded_bid_time     = Udfs.roundTimestampToMinutes(5)(bid_timestamp)
```

- Multi-source join pressure:

```text
bids -> impressions_feedback
bids -> contextual
bids -> advertiser
bids -> koa_settings
bids -> eligible_user_data
bids -> feature_log
bids -> sib
```

- Broadcast joins for small dimensions:

```text
advertiser
koa_settings
```

- Manual skew handling:

```text
split high-frequency users
split low-frequency users
apply salt to high-frequency branch
repartition by salted user/context key
aggregate high and low branches separately
union branches
sort final output
```

- Persisting a wide joined DataFrame:

```text
MEMORY_AND_DISK
```

Purpose in the baseline:

```text
Create the main before-DAG evidence point: wide rows, expensive joins, explicit shuffles, skewed high-frequency branch, manual salting, union, sort, and Parquet output.
```

Tiny-run evidence:

```text
brbf_job.source_bids_count=100000
brbf_job.source_impressions_count=70000
brbf_job.source_feature_log_count=50000
brbf_job.source_sib_count=16200
brbf_job.joined_row_count=1080000
brbf_job.high_frequency_joined_count=1000000
brbf_job.low_frequency_joined_count=80000
brbf_job.final_output_count=91883
```

What this proves:

```text
100,000 bids expanded to 1,080,000 joined rows.
The high-frequency branch dominates the joined workload.
Manual salting is exercised.
The final aggregation reduces the expanded join output to 91,883 rows.
```

Future Databricks comparison opportunities:

```text
replace UDFs with native expressions where possible
use AQE and skew-aware joins
reduce manual salting/repartition code
defer nonessential derived columns
compare Parquet output to Delta layout
evaluate Photon-friendly joins, scans, aggregations, and sorts
```

## Shared Problem Across Steps 08 And 09

Both converter steps intentionally introduce the same broad baseline pattern:

```text
Read raw Parquet
Compute UDF-derived keys early
Add downstream helper columns early
Deduplicate
Manually repartition
Write converted Parquet
```

This gives the future migration comparison a clear before/after story:

```text
Before:
  manual UDF-heavy EMR preprocessing jobs that create wide converted datasets and explicit shuffles

After:
  optimized Databricks pipeline with more native expressions, better planning, improved layout, and less manual shuffle/skew code
```

## Next Step Impact

Step 10, `BrbfJob`, will consume both converted datasets:

```text
converted/feature_log
converted/eligible_user_data
```

The main BRBF job should preserve the baseline story by:

- computing more columns early
- joining multiple large/skewed datasets
- using high/low frequency branches
- applying manual salting
- unioning branches
- sorting final output
- writing Parquet to S3
