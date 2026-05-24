# Step 10: BRBF Job

## Purpose

Implement the main EMR baseline job:

```text
com.demo.emr.BrbfJob
```

This job reproduces the BRBF-style before-DAG:

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

## Scala Entry Point

Added:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/BrbfJob.scala
```

## Execution Order Summary

The code runs in this order:

```text
1. Read raw and converted datasets
2. Prepare each dataset with early transformations
3. Join all supporting datasets to bids
4. Add post-join transformation columns
5. Split into high-frequency and low-frequency branches
6. Aggregate each branch
7. Union and sort final results
8. Write final output
```

Important nuance:

```text
Some transformations happen before joins.
Some transformations happen after joins.
```

The EMR baseline intentionally computes many bid-derived columns before the large joins.

That creates wider rows earlier in the pipeline, which increases join and shuffle pressure.

## Simple Code Walkthrough

`BrbfJob.scala` is the core BRBF transformation.

In simple language, it takes raw bids, enriches them with all supporting datasets, handles high-frequency user skew, aggregates the result, and writes the final BRBF output.

### 1. Start The Job

The job starts in:

```scala
def main(args: Array[String]): Unit
```

This is where `spark-submit` enters the program.

It does three things:

```text
read command-line arguments
create a Spark session
call run(spark, config)
```

### 2. Set Baseline Spark Config

The job sets default Spark config values only when they were not provided by `spark-submit --conf`:

```scala
setDefaultSparkConf(spark, "spark.sql.adaptive.enabled", "false")
setDefaultSparkConf(spark, "spark.sql.adaptive.skewJoin.enabled", "false")
setDefaultSparkConf(spark, "spark.sql.shuffle.partitions", "200")
setDefaultSparkConf(spark, "spark.default.parallelism", "200")
```

This intentionally keeps the job as an EMR before-migration baseline.

AQE and automatic skew handling are disabled so that the manual salting and shuffle pressure remain visible for later comparison.

For controlled reruns, submit-time configs can override these defaults. For example, the medium disk-pressure rerun can use AQE and a higher shuffle partition count without editing this source file again.

### 3. Read The Input Datasets

The job reads:

```text
bids
impressions_feedback
contextual
advertiser
koa_settings
converted eligible_user_data
converted feature_log
sib
```

This is why Step 10 depends on Step 8 and Step 9.

Step 10 needs:

```text
converted/feature_log
converted/eligible_user_data
```

### 4. Prepare Each Dataset

Each `prepare...` method selects or creates the columns needed for the main join.

The biggest preparation is `prepareBids`.

It adds derived columns such as:

```text
normalized_user_id
user_id_hash
bid_request_hash
contextual_join_key
rounded_bid_time
bid_price_bucket
event_quality_score
campaign_grouping_key
```

In business terms, the bid records are enriched with helper fields used for joins, grouping, scoring, and reporting.

This is intentionally inefficient for the baseline because many derived columns are computed before the large joins.

That makes each bid row wider earlier in the pipeline.

### 5. Print Source Counts

The job prints source counts like:

```text
brbf_job.source_bids_count=100000
brbf_job.source_feature_log_count=50000
```

These are validation breadcrumbs.

They confirm the job read the expected amount of data before doing the expensive work.

### 6. Join Everything Together

The main joined DataFrame starts with bids and attaches:

```text
impression feedback
contextual page/content data
advertiser dimension data
KOA campaign settings
eligible-user data
feature-log events
SIB data
```

In business terms:

```text
Take each bid and attach whether it became an impression,
what context it came from,
which advertiser and campaign it belongs to,
which bidding policy applies,
whether the user is eligible,
what feature-log events match,
and what SIB group the user belongs to.
```

Small dimension tables use broadcast joins:

```text
advertiser
koa_settings
```

The larger joins use normal Spark joins.

### 7. Add Final Calculation Columns

After the joins, the job adds:

```text
final_frequency_bracket
is_high_frequency
effective_bid_modifier
adjusted_bid_price
quality_weighted_price
```

Simple meaning:

```text
decide whether the row is high-frequency or low-frequency
apply the KOA bid modifier
calculate adjusted bid price
calculate quality-weighted bid price
```

### 8. Persist The Wide Joined Data

The job persists the joined DataFrame:

```scala
persist(StorageLevel.MEMORY_AND_DISK)
```

This tells Spark to keep the big joined dataset because it is reused several times.

Spark may keep some data in memory and spill some to disk.

This is part of the baseline story because it can create visible memory and shuffle pressure.

### 9. Print Join Match Metrics

The job prints metrics such as:

```text
joined_row_count
joined_impression_match_count
joined_eligible_user_match_count
joined_feature_log_match_count
joined_sib_match_count
high_frequency_joined_count
low_frequency_joined_count
```

These tell us whether the joins are working and how much skew is present.

### 10. Split High And Low Frequency Users

The joined data is split into two branches:

```text
high-frequency users
low-frequency users
```

High-frequency users can create skew because a small number of users generate many rows.

So the job handles high-frequency users differently from normal users.

### 11. High-Frequency Branch Uses Manual Salting

For high-frequency rows, the job creates:

```text
salt
salted_user_key
```

The idea is:

```text
take one hot user
spread that user's rows across 16 salt buckets
reduce the chance that one Spark task gets overloaded
```

This is manual skew handling.

It is intentionally part of the EMR baseline because later we can compare it with AQE and Databricks skew handling.

### 12. Low-Frequency Branch Uses Normal Hash Path

Low-frequency rows are not salted.

They are repartitioned by:

```text
user_id_hash
contextual_id
```

Then they are grouped normally.

### 13. Aggregate Business Metrics

Both branches calculate the same metrics:

```text
event_count
bid_count
impression_count
eligible_user_count
feature_event_count
avg_bid_price
avg_clearing_price
avg_quality_weighted_price
avg_sib_score
```

In business terms, the job summarizes bid, impression, user, feature, and SIB behavior by advertiser, campaign, context, and frequency group.

### 14. Union And Sort Final Output

After high-frequency and low-frequency branches are aggregated, the job combines them:

```scala
highFrequencyAgg.unionByName(lowFrequencyAgg)
```

Then it sorts by:

```text
advertiser_id
campaign_id
contextual_id
frequency_bracket
branch
```

The union and sort add more work near the end of the job, which gives us useful baseline evidence.

### 15. Write Final Output

The final output is written as Parquet to:

```text
final/brbf/year=2026/month=05/day=21/hour=10/
```

For the current bucket, that means:

```text
s3://aigithub-emr-2026/emr-migration-demo/final/brbf/year=2026/month=05/day=21/hour=10/
```

### One-Sentence Summary

`BrbfJob` takes raw bids, enriches them with impressions, context, advertiser settings, eligible-user data, feature logs, and SIB data, then handles high-frequency user skew with manual salting, aggregates business metrics, sorts the result, and writes the final BRBF output.

This is intentionally a before-migration job:

```text
UDFs
early derived columns
manual salting
manual repartitioning
persisted wide joined data
union
sort
Parquet-on-S3 output
```

Those baseline traits give us something clear to improve later with Databricks, AQE, native expressions, Delta layout, and Photon.

Detailed step-by-step validation commands are captured in:

```text
KT/10_brbf_job_validation.md
```

## Inputs

Raw hourly inputs:

```text
raw/bids/year=2026/month=05/day=21/hour=10/
raw/impressions_feedback/year=2026/month=05/day=21/hour=10/
raw/impressions_feedback/year=2026/month=05/day=21/hour=11/
raw/contextual/year=2026/month=05/day=21/hour=10/
```

Raw daily inputs:

```text
raw/advertiser/year=2026/month=05/day=21/
raw/koa_settings/year=2026/month=05/day=21/
raw/sib/year=2026/month=05/day=21/
```

Converted hourly inputs:

```text
converted/feature_log/year=2026/month=05/day=21/hour=10/
converted/eligible_user_data/year=2026/month=05/day=21/hour=10/
```

## Output

```text
final/brbf/year=2026/month=05/day=21/hour=10/
```

## Implemented Flow

The job:

- reads bids
- reads same-hour and late-hour impressions
- reads contextual data
- reads advertiser dimension
- reads KOA settings dimension
- reads converted eligible-user data
- reads converted feature-log data
- reads SIB data
- computes bid-derived columns early
- joins bids to impressions
- joins contextual data
- broadcast joins advertiser
- broadcast joins KOA settings
- joins eligible-user data
- joins feature-log data
- joins SIB data
- persists the wide joined DataFrame with `MEMORY_AND_DISK`
- splits high-frequency and low-frequency users
- manually salts high-frequency users
- repartitions both branches
- aggregates both branches
- unions both branches
- sorts final output
- writes final Parquet

## Baseline Spark Settings

The job sets the before-DAG baseline behavior explicitly:

```scala
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
```

For larger EMR runs, these may be changed to `400` based on cluster size.

## Early Derived Bid Columns

The job intentionally computes columns before major joins:

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

This preserves the baseline problem statement:

```text
wide rows before joins
UDF-derived keys in the critical path
extra shuffle pressure
```

## Join Strategy

Large or medium joins:

```text
bids -> impressions_feedback by bid_request_id
bids -> contextual by contextual_id
bids -> eligible_user_data by bid_request_id and user_id_hash
bids -> feature_log by user_id_hash and contextual_id
bids -> sib by user_id_hash
```

Broadcast joins:

```text
advertiser
koa_settings
```

## Manual Salting

High-frequency rows use manual salting:

```scala
withColumn("salt", pmod(abs(hash(col("bid_request_id"))), lit(16)))
withColumn("salted_user_key", concat_ws("#", col("user_id_hash"), col("salt")))
repartition(col("salted_user_key"), col("contextual_id"))
```

Low-frequency rows use a normal hash path:

```scala
repartition(col("user_id_hash"), col("contextual_id"))
```

The branches are aggregated separately and then unioned.

## Important Implementation Note: SIB Deduplication

The first tiny smoke run showed memory pressure while counting the joined DataFrame:

```text
MemoryStore: Not enough space to cache ...
Persisting block ... to disk instead.
```

The reason was that raw `sib` contains repeated hot-user rows. Joining SIB after feature-log could multiply high-frequency rows too aggressively for a tiny local smoke test.

The job now deduplicates SIB by `user_id_hash` before joining:

```scala
raw.dropDuplicates("user_id_hash")
```

This still matches the intended SIB behavior:

```text
Use latest/daily available SIB data per user.
```

It also keeps the tiny smoke test useful while preserving skew in bids and feature-log joins.

## Build Command

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/mnt/tmp/m2/repository clean package
```

Observed:

```text
BUILD SUCCESS
```

## Run Command

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

## Observed Tiny-Scale Metrics

Source counts:

```text
brbf_job.source_bids_count=100000
brbf_job.source_impressions_count=70000
brbf_job.source_contextual_count=20000
brbf_job.source_advertiser_count=1000
brbf_job.source_koa_settings_count=1000
brbf_job.source_eligible_user_data_count=20000
brbf_job.source_feature_log_count=50000
brbf_job.source_sib_count=16200
```

Join and split counts:

```text
brbf_job.joined_row_count=1080000
brbf_job.joined_impression_match_count=756000
brbf_job.joined_eligible_user_match_count=216000
brbf_job.joined_feature_log_match_count=1040000
brbf_job.joined_sib_match_count=1016000
brbf_job.high_frequency_joined_count=1000000
brbf_job.low_frequency_joined_count=80000
```

Final output:

```text
brbf_job.final_output_count=91883
```

Validation conclusion:

```text
PASS
```

The main BRBF tiny smoke test completed and wrote final Parquet output.

## Baseline Evidence Created

The tiny run already shows the shape of the future baseline:

```text
100,000 bids expanded to 1,080,000 joined rows
1,000,000 joined rows are high-frequency
manual salting branch is exercised
feature-log join creates visible row expansion
SIB join remains selective after daily/user deduplication
final aggregate reduces output to 91,883 rows
```

For larger data, use Spark UI / History Server to capture:

```text
Exchange operators
shuffle read/write
spill
task duration variance
join strategies
sort pressure
union pressure
```

Reference:

```text
KT/spark_troubleshooting/emr_spark_troubleshooting_guide.md
```

## Next Recommended Step

Step 11:

```text
Package, upload JAR to S3, and run the three jobs as EMR steps.
```
