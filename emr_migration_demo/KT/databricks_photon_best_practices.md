# Databricks Photon Best Practices

## Purpose

This document tracks best practices for making the post-migration Databricks version benefit from Photon.

It is based on the EMR baseline we have built so far and should be updated as the Databricks implementation evolves.

## Photon Migration Goal

Use Photon to accelerate the parts of the workload that are most likely to benefit:

```text
file scans
filters
projections
joins
aggregations
sorts
Delta/Parquet reads and writes
```

The EMR baseline intentionally includes operations that give Photon a meaningful comparison point:

```text
wide Parquet scans
projection-heavy transformations
multi-way joins
aggregations
sorts
unioned branches
```

## Current EMR Baseline Operators

The current pipeline uses:

```text
FeatureLogConverter
EligibleUserDataLogConverter
BrbfJob
```

Expected Spark physical operators include:

```text
FileScan parquet
Project
Filter
Exchange
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
HashAggregate
SortAggregate
Sort
Union
Write parquet
Scala UDF projections
```

Photon is most relevant for the native SQL/DataFrame operators, not opaque custom UDF logic.

## Best Practice 1: Prefer Native Spark SQL/DataFrame Expressions

### Baseline Pattern

The EMR baseline intentionally uses Scala UDFs:

```text
Udfs.sha256String
Udfs.stableUuidFromString
Udfs.roundTimestampToMinutes
```

### Photon-Friendly Direction

Replace UDFs with native expressions where possible.

Examples:

```text
sha256String        -> sha2(col, 256)
round timestamp     -> date_trunc / unix_timestamp arithmetic / window-compatible expressions
bucket columns      -> when/otherwise native expressions
string cleanup      -> lower, trim, concat_ws
```

Why:

```text
Native expressions give Spark and Photon more visibility into execution.
Opaque UDFs can limit optimization and vectorized execution benefits.
```

## Best Practice 2: Keep Work In DataFrame/SQL Operators

Favor:

```text
select
withColumn using native functions
filter
join
groupBy
agg
orderBy/sort
unionByName
```

Avoid:

```text
row-by-row custom logic
RDD transformations for tabular logic
collecting data to the driver
large driver-side maps/lists
custom serialization-heavy operations
```

Why:

```text
Photon accelerates supported SQL/DataFrame execution paths.
Dropping into custom code reduces the opportunity for Photon acceleration.
```

Related future migration story:

```text
KT/future_migration_stories.md
Story 2: HyperMinHash Row-By-Row Merge To Spark Expressions
```

This is the clearest example so far of a pattern where Photon can help the surrounding reads, filters, aggregations, and writes, but the biggest gain likely requires rewriting row-by-row JVM merge logic into Spark-native expressions.

## Best Practice 3: Use Delta Lake For Migrated Tables

### Baseline Pattern

Current EMR baseline:

```text
Parquet on S3
Snappy compression
manual partition paths
```

### Photon-Friendly Direction

Use Delta tables for Databricks-managed or lakehouse outputs:

```text
Delta for converted/feature_log
Delta for converted/eligible_user_data
Delta for final/brbf
```

Benefits:

```text
table metadata
statistics
better query planning
optimized reads
easier validation and time travel
potential file layout improvements
```

## Best Practice 4: Optimize File Layout

Photon can read data quickly, but poor file layout can still hurt.

Watch for:

```text
too many small files
very large uneven files
poor clustering for common filters/joins
unnecessary partition nesting
```

For this workload, important columns may include:

```text
run date partitions
hour partitions
user_id_hash
contextual_id
advertiser_id
campaign_id
frequency_bracket
```

Future Databricks layout options:

```text
optimized writes
auto compaction where available
OPTIMIZE
ZORDER where appropriate
liquid clustering where appropriate
```

Use these only after measuring query patterns.

## Best Practice 5: Use AQE And Databricks Runtime Optimizations

### Baseline Pattern

The EMR baseline disables AQE:

```text
spark.sql.adaptive.enabled=false
spark.sql.adaptive.skewJoin.enabled=false
```

### Databricks Direction

For the optimized Databricks run, evaluate:

```text
AQE enabled
skew join handling enabled
auto broadcast behavior
dynamic partition pruning where applicable
optimized shuffle settings
```

Why:

```text
AQE can reduce the need for manual repartitioning and salting.
Photon plus AQE can improve supported joins, aggregations, and scans.
```

## Best Practice 6: Revisit Manual Salting

### Baseline Pattern

`BrbfJob` uses manual salting for high-frequency users:

```text
salt = pmod(abs(hash(bid_request_id)), 16)
salted_user_key = user_id_hash + "#" + salt
repartition(salted_user_key, contextual_id)
```

### Databricks Direction

Test whether Databricks AQE/skew handling reduces or eliminates the need for manual salting.

Compare:

```text
manual salting enabled
manual salting removed
manual salting reduced
AQE skew handling enabled
```

Metrics:

```text
runtime
shuffle read/write
task duration variance
spill
physical plan
output count correctness
```

## Best Practice 7: Defer Nonessential Derived Columns

### Baseline Pattern

The EMR baseline computes many derived columns before joins:

```text
normalized_user_id
user_id_hash
user_uuid
bid_request_hash
contextual_join_key
rounded_bid_time
bid_price_bucket
event_quality_score
campaign_grouping_key
```

### Databricks Direction

Classify derived columns:

```text
needed for joins
needed for filters
needed only for final output
needed only for diagnostics
```

Then defer columns that are not needed before large joins.

Why:

```text
Slimmer rows can reduce shuffle size and memory pressure.
Photon can still accelerate projections, but avoiding unnecessary early width is better.
```

## Best Practice 8: Broadcast Only True Dimensions

Baseline broadcast joins:

```text
advertiser
koa_settings
```

Databricks direction:

```text
let AQE choose broadcast where possible
use hints only when justified
validate actual plan
avoid broadcasting unexpectedly large data
```

Verify in the query profile:

```text
BroadcastHashJoin
BroadcastExchange
```

## Best Practice 9: Reduce Unnecessary Shuffles

Baseline explicit shuffle sources:

```text
dropDuplicates
repartition
joins
groupBy
sort
union branch aggregation
```

Databricks direction:

```text
remove redundant repartition calls
combine compatible aggregations
filter earlier when possible
avoid global sort unless required
use sortWithinPartitions if global ordering is unnecessary
```

Photon may speed supported operators, but fewer unnecessary shuffles is still better.

## Best Practice 10: Use Query Profile To Confirm Photon

In Databricks, use:

```text
Spark UI
Databricks Query Profile
SQL/DataFrame physical plan
job run metrics
```

Look for:

```text
Photon enabled
Photon-supported operators
time spent in scans
time spent in joins
time spent in aggregations
shuffle read/write
spill
task skew
```

Compare against EMR Spark History Server evidence:

```text
same input scale
same run date/hour
same business output counts
similar or improved join match rates
lower runtime
lower shuffle/spill
better task balance
```

## Best Practice 11: Separate Lifted-JAR And Optimized Runs

Use three comparison labels:

```text
emr_baseline
databricks_lifted_jar
databricks_optimized
```

Why:

```text
The lifted JAR proves platform portability.
The optimized version proves migration improvement.
```

Do not mix these stories too early.

## Best Practice 12: Keep Output Validation Strict

Photon or Databricks optimizations should not change business outputs unexpectedly.

For each run, compare:

```text
converted feature_log count
converted eligible_user_data count
final brbf count
high/low frequency counts
late feedback counts
join match rates
important null rates
duplicate final keys
aggregate totals
```

Any performance improvement must preserve correctness.

## Mapping Current Baseline To Photon Opportunities

### FeatureLogConverter

Current baseline:

```text
Parquet scan
UDF hash
UDF UUID
UDF timestamp rounding
feature value bucket
dropDuplicates
repartition
Parquet write
```

Photon opportunity:

```text
native sha2
native timestamp bucketing
native projections
Delta output
layout optimization
```

### EligibleUserDataLogConverter

Current baseline:

```text
Parquet scan
UDF hash
UDF UUID
bid request hash
eligibility band
frequency bracket
dropDuplicates
repartition
Parquet write
```

Photon opportunity:

```text
native sha2
native projections
Delta output
avoid unnecessary repartition if AQE/layout handles it
```

### BrbfJob

Current baseline:

```text
multiple Parquet scans
early UDF-derived columns
impression join
contextual join
broadcast advertiser join
broadcast KOA settings join
eligible user join
feature-log join
SIB join
persist wide joined DataFrame
high/low split
manual salting
aggregations
union
sort
Parquet write
```

Photon opportunity:

```text
native expressions
AQE/skew joins
fewer manual repartitions
remove/reduce salting if possible
Photon-supported joins
Photon-supported aggregates
Photon-supported scans
Delta output and optimized layout
```

## Success Criteria

Photon and Databricks optimization story is successful if we can show:

```text
same or validated output counts
lower runtime
less shuffle read/write
less spill
better task balance
simpler code
fewer manual skew-handling steps
better file/table layout
clear query profile evidence
```

## Current Decision

For now:

```text
Do not modify the EMR baseline to be Photon-friendly.
Keep the EMR baseline intentionally old-school.
Use this document when building the Databricks optimized version.
```

This keeps the before/after story clean.
