# Step 10: BRBF Job Validation

## Purpose

This document captures the step-by-step validation flow for the core BRBF job:

```text
com.demo.emr.BrbfJob
```

The goal is to understand and validate the main logic in small chunks:

```text
read inputs
prepare bids
prepare supporting datasets
join datasets
add post-join columns
split high/low frequency users
salt high-frequency users
aggregate
union
write final output
```

## Spark Shell Setup

Start from the project folder:

```bash
cd /home/hadoop/emr_migration_demo
spark-shell
```

Inside `spark-shell`:

```scala
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.debug.maxToStringFields", "200")
```

Use `:paste` for multi-line DataFrame chains.

Important Spark shell note:

```text
If you type:

val df =
  baseDf

and press enter, Spark creates df as baseDf.
The following .withColumn or .join lines become separate temporary values.

Use :paste for multi-line chains, or keep simple chains on one line.
```

## Validation 1: Read Raw Bids

Code validated:

```scala
DatasetIO.readRaw(spark, config, "bids")
```

Command:

```scala
val rawBidsPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/bids/year=2026/month=05/day=21/hour=10/"

val rawBids = spark.read.parquet(rawBidsPath)

rawBids.select(
  "bid_request_id",
  "user_id",
  "user_id_hash",
  "advertiser_id",
  "campaign_id",
  "contextual_id",
  "bid_timestamp",
  "bid_price",
  "frequency_bracket"
).orderBy("bid_request_id")
 .show(10, false)
```

What this proves:

```text
raw bids exist
Spark can read the partition
required bid columns are present
```

## Validation 2: Prepare Bids

Code validated:

```scala
private def prepareBids(raw: DataFrame): DataFrame
```

First validate one column:

```scala
val preparedBidsCheck2 =
  rawBids.withColumn("normalized_user_id", lower(trim(col("user_id"))))

preparedBidsCheck2.select(
  "bid_request_id",
  "user_id",
  "normalized_user_id"
).show(10, false)
```

Then create the full prepared bid check:

```scala
:paste
```

Paste:

```scala
val preparedBidsCheck3 =
  preparedBidsCheck2
    .withColumn("source_user_id_hash", col("user_id_hash"))
    .withColumn("user_id_hash_recomputed", sha2(col("normalized_user_id"), 256))
    .withColumn("bid_request_hash", sha2(col("bid_request_id"), 256))
    .withColumn("contextual_join_key", sha2(col("contextual_id").cast("string"), 256))
    .withColumn(
      "rounded_bid_time",
      from_unixtime((unix_timestamp(col("bid_timestamp")) / 300).cast("long") * 300).cast("timestamp")
    )
    .withColumn(
      "bid_price_bucket",
      when(col("bid_price") >= 4.0d, lit("premium"))
        .when(col("bid_price") >= 2.0d, lit("standard"))
        .otherwise(lit("low"))
    )
    .withColumn(
      "event_quality_score",
      when(col("is_test_bid") === true, lit(0.10d))
        .when(col("bid_price") >= 4.0d, lit(0.95d))
        .otherwise(lit(0.70d))
    )
    .withColumn("campaign_grouping_key", concat_ws(":", col("advertiser_id"), col("campaign_id")))
    .withColumnRenamed("frequency_bracket", "bid_frequency_bracket")
```

Press `Ctrl+D`.

Verify columns:

```scala
preparedBidsCheck3.columns.sorted.foreach(println)
```

Expected added columns:

```text
normalized_user_id
source_user_id_hash
user_id_hash_recomputed
bid_request_hash
contextual_join_key
rounded_bid_time
bid_price_bucket
event_quality_score
campaign_grouping_key
bid_frequency_bracket
```

Inspect in readable chunks:

```scala
preparedBidsCheck3.select(
  "bid_request_id",
  "user_id",
  "normalized_user_id",
  "source_user_id_hash",
  "user_id_hash_recomputed"
).show(5, false)
```

```scala
preparedBidsCheck3.select(
  "bid_request_id",
  "contextual_id",
  "contextual_join_key",
  "bid_timestamp",
  "rounded_bid_time"
).show(5, false)
```

```scala
preparedBidsCheck3.select(
  "bid_request_id",
  "advertiser_id",
  "campaign_id",
  "campaign_grouping_key",
  "bid_price",
  "bid_price_bucket",
  "event_quality_score",
  "bid_frequency_bracket"
).show(5, false)
```

Note:

```text
The actual Scala job overwrites user_id_hash with the recomputed hash.
The manual validation uses user_id_hash_recomputed so original and recomputed values can be compared side by side.
```

## Validation 3: Prepare Supporting Datasets

Code validated:

```scala
prepareImpressions(...)
prepareContextual(...)
prepareAdvertiser(...)
prepareKoaSettings(...)
prepareEligibleUserData(...)
prepareFeatureLog(...)
prepareSib(...)
```

Paths:

```scala
val impressions10Path =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/impressions_feedback/year=2026/month=05/day=21/hour=10/"

val impressions11Path =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/impressions_feedback/year=2026/month=05/day=21/hour=11/"

val contextualPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/contextual/year=2026/month=05/day=21/hour=10/"

val advertiserPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/advertiser/year=2026/month=05/day=21/"

val koaPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/koa_settings/year=2026/month=05/day=21/"

val eligiblePath =
  "s3://aigithub-emr-2026/emr-migration-demo/converted/eligible_user_data/year=2026/month=05/day=21/hour=10/"

val featureLogPath =
  "s3://aigithub-emr-2026/emr-migration-demo/converted/feature_log/year=2026/month=05/day=21/hour=10/"

val sibPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/sib/year=2026/month=05/day=21/"
```

Prepared checks:

```scala
val impressionsCheck =
  spark.read.parquet(impressions10Path)
    .unionByName(spark.read.parquet(impressions11Path))
    .dropDuplicates("bid_request_id")
    .select(
      "bid_request_id",
      "impression_id",
      "feedback_timestamp",
      "is_billable",
      "clearing_price",
      "feedback_type"
    )

val contextualCheck =
  spark.read.parquet(contextualPath)
    .select(
      "contextual_id",
      "domain",
      "content_category",
      "is_hot_context",
      "context_score"
    )

val advertiserCheck =
  spark.read.parquet(advertiserPath)
    .select(
      "advertiser_id",
      "advertiser_name",
      "vertical",
      "priority_tier",
      "is_active"
    )

val koaCheck =
  spark.read.parquet(koaPath)
    .select(
      "advertiser_id",
      "campaign_id",
      "koa_group",
      "bid_modifier",
      "policy_mode"
    )

val eligibleCheck =
  spark.read.parquet(eligiblePath)
    .select(
      "bid_request_id",
      "user_id_hash",
      "segment_id",
      "is_eligible",
      "eligible_user_flag",
      "eligibility_score",
      "eligibility_band",
      "frequency_bracket"
    )

val featureLogCheck =
  spark.read.parquet(featureLogPath)
    .select(
      "feature_event_id",
      "user_id_hash",
      "contextual_id",
      "feature_name",
      "feature_value",
      "feature_value_bucket",
      "rounded_event_time"
    )

val sibCheck =
  spark.read.parquet(sibPath)
    .dropDuplicates("user_id_hash")
    .select(
      "user_id_hash",
      "sib_group",
      "sib_score",
      "sib_source"
    )
```

Counts:

```scala
println("impressionsCheck=" + impressionsCheck.count())
println("contextualCheck=" + contextualCheck.count())
println("advertiserCheck=" + advertiserCheck.count())
println("koaCheck=" + koaCheck.count())
println("eligibleCheck=" + eligibleCheck.count())
println("featureLogCheck=" + featureLogCheck.count())
println("sibCheck=" + sibCheck.count())
```

Expected tiny-run counts:

```text
impressionsCheck=70000
contextualCheck=20000
advertiserCheck=1000
koaCheck=1000
eligibleCheck=20000
featureLogCheck=50000
sibCheck=16200
```

## Validation 4: Join Sample Bids

Code validated:

```scala
val joined = bids
  .join(impressions, Seq("bid_request_id"), "left")
  .join(contextual, Seq("contextual_id"), "left")
  .join(broadcast(advertiser), Seq("advertiser_id"), "left")
  .join(broadcast(koaSettings), Seq("advertiser_id", "campaign_id"), "left")
  .join(eligibleUserData, Seq("bid_request_id", "user_id_hash"), "left")
  .join(featureLog, ...)
  .join(sib, ...)
```

Create renamed join DataFrames:

```scala
val eligibleJoinCheck2 =
  eligibleCheck.withColumnRenamed("is_eligible", "eligible_user_is_eligible")
    .withColumnRenamed("frequency_bracket", "eligible_frequency_bracket")

val featureLogJoinCheck2 =
  featureLogCheck.withColumnRenamed("user_id_hash", "feature_user_id_hash")
    .withColumnRenamed("contextual_id", "feature_contextual_id")
    .withColumnRenamed("rounded_event_time", "feature_rounded_event_time")

val sibJoinCheck2 =
  sibCheck.withColumnRenamed("user_id_hash", "sib_user_id_hash")
```

Create a small bid sample:

```scala
val bidsSampleForJoin2 =
  preparedBidsCheck3.filter(col("bid_request_id").isin("br_0", "br_1", "br_2", "br_3", "br_4"))

bidsSampleForJoin2.count()
```

Expected:

```text
5
```

Create joined sample:

```scala
:paste
```

Paste:

```scala
val joinedSample2 =
  bidsSampleForJoin2
    .join(impressionsCheck, Seq("bid_request_id"), "left")
    .join(contextualCheck, Seq("contextual_id"), "left")
    .join(broadcast(advertiserCheck), Seq("advertiser_id"), "left")
    .join(broadcast(koaCheck), Seq("advertiser_id", "campaign_id"), "left")
    .join(eligibleJoinCheck2, Seq("bid_request_id", "user_id_hash"), "left")
    .join(
      featureLogJoinCheck2,
      bidsSampleForJoin2("user_id_hash") === featureLogJoinCheck2("feature_user_id_hash") &&
        bidsSampleForJoin2("contextual_id") === featureLogJoinCheck2("feature_contextual_id"),
      "left"
    )
    .join(
      sibJoinCheck2,
      bidsSampleForJoin2("user_id_hash") === sibJoinCheck2("sib_user_id_hash"),
      "left"
    )
```

Press `Ctrl+D`.

Count:

```scala
joinedSample2.count()
```

Observed:

```text
5000
```

Interpretation:

```text
5 bid rows expanded to 5000 rows.
This is expected because the feature-log join can multiply rows for hot user/context keys.
```

Inspect impression/context/advertiser/KOA joins:

```scala
joinedSample2.select(
  "bid_request_id",
  "advertiser_id",
  "campaign_id",
  "contextual_id",
  "impression_id",
  "domain",
  "content_category",
  "advertiser_name",
  "koa_group",
  "bid_modifier"
).show(20, false)
```

Observed example for `br_2`:

```text
impression_id=imp_2
domain=domain_2.example
content_category=category_2
advertiser_name=advertiser_2
koa_group=koa_group_2
bid_modifier=0.02
```

Inspect eligible join:

```scala
joinedSample2.select(
  "bid_request_id",
  "segment_id",
  "eligible_user_flag",
  "eligible_frequency_bracket"
).show(20, false)
```

Observed example:

```text
br_2 segment_id=segment_2 eligible_user_flag=0 eligible_frequency_bracket=high
```

Inspect feature-log join:

```scala
joinedSample2.select(
  "bid_request_id",
  "feature_event_id",
  "feature_name",
  "feature_value_bucket"
).show(20, false)
```

Observed example:

```text
br_2 feature_event_id=feature_event_9002 feature_name=feature_2 feature_value_bucket=low
```

Inspect SIB join:

```scala
joinedSample2.select(
  "bid_request_id",
  "sib_group",
  "sib_score"
).show(20, false)
```

Observed example:

```text
br_2 sib_group=sib_group_2 sib_score=0.002
```

## Validation 5: Add Post-Join Columns

Code validated:

```scala
.withColumn("final_frequency_bracket", coalesce($"eligible_frequency_bracket", $"bid_frequency_bracket"))
.withColumn("is_high_frequency", $"final_frequency_bracket" === lit("high"))
.withColumn("effective_bid_modifier", coalesce($"bid_modifier", lit(1.0d)))
.withColumn("adjusted_bid_price", $"bid_price" * $"effective_bid_modifier")
.withColumn("quality_weighted_price", $"adjusted_bid_price" * $"event_quality_score")
```

Command:

```scala
:paste
```

Paste:

```scala
val joinedWithPostColumns2 =
  joinedSample2
    .withColumn("final_frequency_bracket", coalesce(col("eligible_frequency_bracket"), col("bid_frequency_bracket")))
    .withColumn("is_high_frequency", col("final_frequency_bracket") === lit("high"))
    .withColumn("effective_bid_modifier", coalesce(col("bid_modifier"), lit(1.0d)))
    .withColumn("adjusted_bid_price", col("bid_price") * col("effective_bid_modifier"))
    .withColumn("quality_weighted_price", col("adjusted_bid_price") * col("event_quality_score"))
```

Press `Ctrl+D`.

Verify columns:

```scala
joinedWithPostColumns2.columns.sorted.foreach(println)
```

Expected added columns:

```text
final_frequency_bracket
is_high_frequency
effective_bid_modifier
adjusted_bid_price
quality_weighted_price
```

Inspect frequency logic:

```scala
joinedWithPostColumns2.select(
  "bid_request_id",
  "bid_frequency_bracket",
  "eligible_frequency_bracket",
  "final_frequency_bracket",
  "is_high_frequency"
).show(20, false)
```

Inspect price logic:

```scala
joinedWithPostColumns2.select(
  "bid_request_id",
  "bid_price",
  "bid_modifier",
  "effective_bid_modifier",
  "adjusted_bid_price",
  "event_quality_score",
  "quality_weighted_price"
).show(20, false)
```

## Validation 6: High/Low Frequency Aggregation Example

This example explains what happens immediately before and during:

```scala
aggregateHighFrequency(joined.filter($"is_high_frequency"))
aggregateLowFrequency(joined.filter(!$"is_high_frequency"))
```

### How The Tiny Sample Gets To Aggregation

The aggregation functions do not run on raw bids.

They run after the data has already gone through:

```text
raw bids
prepareBids early columns
joins to supporting datasets
post-join calculated columns
high/low frequency split
```

Start with four simple raw bids:

```text
bid_request_id | user_id | advertiser_id | campaign_id | contextual_id | bid_price | frequency_bracket
br_1           | userA   | 1             | 100         | 10            | 2.00      | high
br_2           | userA   | 1             | 100         | 10            | 3.00      | high
br_3           | userB   | 2             | 200         | 20            | 1.00      | low
br_4           | userC   | 2             | 200         | 20            | 5.00      | low
```

After `prepareBids`, the same bids have helper columns:

```text
bid_request_id | user_id | user_id_hash | contextual_join_key | bid_price_bucket | event_quality_score | campaign_grouping_key | bid_frequency_bracket
br_1           | userA   | hash_userA   | hash_10             | standard         | 0.70                | 1:100                 | high
br_2           | userA   | hash_userA   | hash_10             | standard         | 0.70                | 1:100                 | high
br_3           | userB   | hash_userB   | hash_20             | low              | 0.70                | 2:200                 | low
br_4           | userC   | hash_userC   | hash_20             | premium          | 0.95                | 2:200                 | low
```

After impression, contextual, advertiser, and KOA joins:

```text
bid_request_id | impression_id | content_category | advertiser_name | koa_group | bid_modifier
br_1           | imp_1         | sports           | advertiser_1    | koa_A     | 1.10
br_2           | imp_2         | sports           | advertiser_1    | koa_A     | 1.10
br_3           | null          | news             | advertiser_2    | koa_B     | 0.90
br_4           | imp_4         | news             | advertiser_2    | koa_B     | 0.90
```

After eligible-user join:

```text
bid_request_id | segment_id | eligible_user_flag | eligible_frequency_bracket
br_1           | segment_A  | 1                  | high
br_2           | segment_A  | 0                  | high
br_3           | segment_B  | 1                  | low
br_4           | segment_C  | 1                  | low
```

After feature-log join, rows can expand.

Example:

```text
bid_request_id | user_id_hash | contextual_id | feature_event_id
br_1           | hash_userA   | 10            | feat_1
br_1           | hash_userA   | 10            | feat_2
br_2           | hash_userA   | 10            | feat_1
br_2           | hash_userA   | 10            | feat_2
br_3           | hash_userB   | 20            | feat_3
br_4           | hash_userC   | 20            | null
```

This is the same pattern we saw in the real validation:

```text
5 sample bids became 5000 joined rows
```

After SIB join:

```text
bid_request_id | sib_group   | sib_score
br_1           | sib_group_A | 0.50
br_2           | sib_group_A | 0.70
br_3           | sib_group_B | 0.20
br_4           | sib_group_C | 0.90
```

After post-join calculated columns:

```text
bid_request_id | final_frequency_bracket | is_high_frequency | bid_price | bid_modifier | adjusted_bid_price | quality_weighted_price
br_1           | high                    | true              | 2.00      | 1.10         | 2.20               | 1.54
br_2           | high                    | true              | 3.00      | 1.10         | 3.30               | 2.31
br_3           | low                     | false             | 1.00      | 0.90         | 0.90               | 0.63
br_4           | low                     | false             | 5.00      | 0.90         | 4.50               | 4.275
```

This is the point where the job splits:

```scala
joined.filter($"is_high_frequency")
joined.filter(!$"is_high_frequency")
```

### Tiny Input Before Aggregation

Right before aggregation, the data is still at joined event-row level:

```text
bid_request_id | user_id_hash | contextual_id | final_frequency_bracket | is_high_frequency | advertiser_id | campaign_id | content_category | koa_group | feature_event_id | sib_score | quality_weighted_price
br_1           | hash_userA   | 10            | high                    | true              | 1             | 100         | sports           | koa_A     | feat_1           | 0.50      | 1.54
br_1           | hash_userA   | 10            | high                    | true              | 1             | 100         | sports           | koa_A     | feat_2           | 0.50      | 1.54
br_2           | hash_userA   | 10            | high                    | true              | 1             | 100         | sports           | koa_A     | feat_1           | 0.70      | 2.31
br_2           | hash_userA   | 10            | high                    | true              | 1             | 100         | sports           | koa_A     | feat_2           | 0.70      | 2.31
br_3           | hash_userB   | 20            | low                     | false             | 2             | 200         | news             | koa_B     | feat_3           | 0.20      | 0.63
br_4           | hash_userC   | 20            | low                     | false             | 2             | 200         | news             | koa_B     | null             | 0.90      | 4.275
```

### High-Frequency Branch

High-frequency rows:

```text
br_1
br_2
```

The job creates:

```text
salt = hash(bid_request_id) % 16
salted_user_key = user_id_hash + "#" + salt
```

Example:

```text
br_1 -> salt 3 -> hash_userA#3
br_2 -> salt 8 -> hash_userA#8
```

Then it repartitions by:

```text
salted_user_key
contextual_id
```

Then it groups by:

```text
advertiser_id
campaign_id
contextual_id
frequency_bracket
content_category
koa_group
salt
```

Example high output:

```text
advertiser_id | campaign_id | contextual_id | frequency_bracket | content_category | koa_group | salt | event_count | bid_count | impression_count | eligible_user_count | feature_event_count | branch
1             | 100         | 10            | high              | sports           | koa_A     | 3    | 2           | 1         | 2                | 2                   | 2                   | high_frequency_salted
1             | 100         | 10            | high              | sports           | koa_A     | 8    | 2           | 1         | 2                | 0                   | 2                   | high_frequency_salted
```

### Low-Frequency Branch

Low-frequency rows:

```text
br_3
br_4
```

The job does not create a salted key for processing.

It repartitions by:

```text
user_id_hash
contextual_id
```

Then it groups by:

```text
advertiser_id
campaign_id
contextual_id
frequency_bracket
content_category
koa_group
```

Example low output:

```text
advertiser_id | campaign_id | contextual_id | frequency_bracket | content_category | koa_group | salt | event_count | bid_count | impression_count | eligible_user_count | feature_event_count | avg_bid_price | avg_quality_weighted_price | avg_sib_score | branch
2             | 200         | 20            | low               | news             | koa_B     | null | 2           | 2         | 1                | 2                   | 1                   | 3.00          | 2.525                      | 0.55          | low_frequency_hash
```

### Why selectFinalColumns Exists

Both branches need the same schema before:

```scala
highFrequencyAgg.unionByName(lowFrequencyAgg)
```

`selectFinalColumns` forces both outputs into this same order:

```text
advertiser_id
campaign_id
contextual_id
frequency_bracket
content_category
koa_group
salt
branch
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

## Validation 7: Final Output

After running `spark-submit`, read final output:

```scala
val finalBrbfPath =
  "s3://aigithub-emr-2026/emr-migration-demo/final/brbf/year=2026/month=05/day=21/hour=10/"

val finalBrbf = spark.read.parquet(finalBrbfPath)

finalBrbf.count()
```

Expected tiny-run value:

```text
91883
```

Observed:

```text
finalBrbf.count() = 91883
```

Check branches:

```scala
finalBrbf.groupBy("branch").count().show(false)
```

Observed:

```text
+---------------------+-----+
|branch               |count|
+---------------------+-----+
|low_frequency_hash   |72500|
|high_frequency_salted|19383|
+---------------------+-----+
```

Interpretation:

```text
Both final output branches were written successfully.
The high-frequency salted branch exists.
The low-frequency hash branch exists.
The unioned final output can be read back from S3.
```

Inspect high-frequency salted output:

```scala
finalBrbf.filter(col("branch") === "high_frequency_salted")
  .select(
    "advertiser_id",
    "campaign_id",
    "contextual_id",
    "frequency_bracket",
    "salt",
    "event_count",
    "bid_count",
    "feature_event_count"
  )
  .show(20, false)
```

Observed high-frequency salted sample:

```text
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
|advertiser_id|campaign_id|contextual_id|frequency_bracket|salt|event_count|bid_count|feature_event_count|
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
|1            |18701      |1            |high             |9   |50         |1        |50                 |
|1            |18701      |1            |high             |7   |50         |1        |50                 |
|1            |18706      |6            |high             |9   |50         |1        |50                 |
|1            |18706      |6            |high             |14  |50         |1        |50                 |
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
```

Interpretation:

```text
frequency_bracket is high
salt contains real bucket values
event_count and feature_event_count are aggregated
bid_count can remain 1 when one bid expands to many feature-log events
```

This validates:

```scala
.withColumn("salt", pmod(abs(hash(col("bid_request_id"))), lit(SaltBuckets)))
.withColumn("salted_user_key", concat_ws("#", col("user_id_hash"), col("salt")))
.repartition(col("salted_user_key"), col("contextual_id"))
.groupBy(..., col("salt"))
.agg(...)
.withColumn("branch", lit("high_frequency_salted"))
```

Inspect low-frequency output:

```scala
finalBrbf.filter(col("branch") === "low_frequency_hash")
  .select(
    "advertiser_id",
    "campaign_id",
    "contextual_id",
    "frequency_bracket",
    "salt",
    "event_count",
    "bid_count",
    "feature_event_count"
  )
  .show(20, false)
```

Observed low-frequency sample:

```text
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
|advertiser_id|campaign_id|contextual_id|frequency_bracket|salt|event_count|bid_count|feature_event_count|
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
|3            |15123      |23           |low              |NULL|2          |2        |1                  |
|3            |15128      |28           |low              |NULL|2          |2        |1                  |
|3            |15133      |33           |low              |NULL|2          |2        |1                  |
|3            |15138      |5138         |low              |NULL|1          |1        |0                  |
|3            |15138      |15138        |low              |NULL|1          |1        |1                  |
+-------------+-----------+-------------+-----------------+----+-----------+---------+-------------------+
```

Interpretation:

```text
frequency_bracket is low
salt is NULL
low-frequency rows were aggregated without manual salting
event_count, bid_count, and feature_event_count are present
```

This validates:

```scala
.repartition(col("user_id_hash"), col("contextual_id"))
.groupBy(...)
.agg(...)
.withColumn("salt", lit(null).cast("int"))
.withColumn("branch", lit("low_frequency_hash"))
```

Salt sanity check:

```scala
finalBrbf.groupBy("branch", "salt").count().orderBy("branch", "salt").show(50, false)
```

Expected:

```text
high_frequency_salted has salt values
low_frequency_hash has null salt
```

Observed:

```text
+---------------------+----+-----+
|branch               |salt|count|
+---------------------+----+-----+
|high_frequency_salted|0   |1215 |
|high_frequency_salted|1   |1240 |
|high_frequency_salted|2   |1203 |
|high_frequency_salted|3   |1230 |
|high_frequency_salted|4   |1153 |
|high_frequency_salted|5   |1201 |
|high_frequency_salted|6   |1310 |
|high_frequency_salted|7   |1200 |
|high_frequency_salted|8   |1156 |
|high_frequency_salted|9   |1204 |
|high_frequency_salted|10  |1205 |
|high_frequency_salted|11  |1169 |
|high_frequency_salted|12  |1217 |
|high_frequency_salted|13  |1241 |
|high_frequency_salted|14  |1205 |
|high_frequency_salted|15  |1234 |
|low_frequency_hash   |NULL|72500|
+---------------------+----+-----+
```

Interpretation:

```text
High-frequency rows are distributed across all 16 salt buckets.
Low-frequency rows have NULL salt as expected.
The high-frequency salted branch and low-frequency hash branch have compatible final schemas.
```

## Validation Summary

Validated so far:

```text
raw bids read correctly
prepareBids creates early derived columns
supporting datasets are readable and shaped correctly
sample bids join to impressions
sample bids join to contextual
sample bids join to advertiser
sample bids join to KOA settings
sample bids join to eligible_user_data
sample bids join to feature_log
sample bids join to SIB
feature-log join expands rows as expected
post-join columns are created
high/low aggregation logic is understood through example
final output can be read and checked
final branch counts are validated
salt distribution is validated
```

## Common Spark Shell Errors

### Error: Column Not Found After withColumn

Cause:

```text
The multi-line chain did not attach to the val.
```

Fix:

```text
Use :paste and press Ctrl+D after the full block.
```

### Error: value show is not a member of Throwable

Cause:

```text
The previous select failed.
Then a line starting with .show(...) was attached to lastException.
```

Fix:

```text
Rerun the full select(...).show(...) command from the beginning.
```

### Error: Ambiguous user_id

Cause:

```text
After joins, several datasets have user_id columns.
```

Fix:

```text
Select non-ambiguous columns, or rename columns before joining.
```
