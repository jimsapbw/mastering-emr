# Step 09: Eligible User Data Converter

## Purpose

Implement the second EMR pipeline step:

```text
com.demo.emr.EligibleUserDataLogConverter
```

This step reads raw matched-user data, prepares normalized user join keys, adds eligibility and frequency fields, and writes the converted eligible-user dataset for the later BRBF job.

This step also contributes to the broader EMR baseline problem statement:

```text
emr_migration_demo/KT/emr_baseline_problem_statement.md
```

## Source

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/matched_user_data/year=2026/month=05/day=21/hour=10/
```

## Output

```text
s3://aigithub-emr-2026/emr-migration-demo/converted/eligible_user_data/year=2026/month=05/day=21/hour=10/
```

## Scala Entry Point

Added:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/EligibleUserDataLogConverter.scala
```

The converter:

- reads `raw/matched_user_data`
- normalizes `user_id`
- preserves the source hash as `source_user_id_hash`
- recomputes `user_id_hash` with the shared SHA-256 UDF
- creates `user_uuid` with the shared stable UUID UDF
- creates `bid_request_hash`
- derives `frequency_bracket`
- derives `eligibility_band`
- creates numeric `eligible_user_flag`
- deduplicates by `bid_request_id` and `user_id_hash`
- repartitions by `user_id_hash` and `frequency_bracket`
- writes Snappy Parquet to `converted/eligible_user_data`

## Code Walkthrough

`EligibleUserDataLogConverter` prepares user eligibility data for the main BRBF job. The raw dataset is called `matched_user_data`, but the converted output is called `eligible_user_data` because it is shaped for downstream eligibility joins.

The object is the application entry point:

```scala
object EligibleUserDataLogConverter
```

When this class is passed to `spark-submit` with:

```bash
--class com.demo.emr.EligibleUserDataLogConverter
```

Spark starts from this object's `main` method.

The `main` method parses runtime arguments, creates the Spark session, and calls `run`:

```scala
val config = ArgsParser.parse(args)
val spark = SparkSessionFactory.create("emr-migration-eligible-user-data-converter")
run(spark, config)
```

The raw matched-user dataset is read here:

```scala
val rawMatchedUserData = DatasetIO.readRaw(spark, config, "matched_user_data")
```

For the current tiny run, that points to:

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/matched_user_data/year=2026/month=05/day=21/hour=10/
```

The main conversion starts from:

```scala
val converted = rawMatchedUserData
```

This cleans the user id:

```scala
.withColumn("normalized_user_id", lower(trim($"user_id")))
```

This keeps the raw hash and recomputes the canonical join hash:

```scala
.withColumn("source_user_id_hash", $"user_id_hash")
.withColumn("user_id_hash", Udfs.sha256String($"normalized_user_id"))
```

This creates a stable UUID-style user key:

```scala
.withColumn("user_uuid", Udfs.stableUuidFromString($"normalized_user_id"))
```

This creates a hashed bid-request key:

```scala
.withColumn("bid_request_hash", Udfs.sha256String($"bid_request_id"))
```

This creates the high/low frequency split used by the main BRBF job:

```scala
.withColumn(
  "frequency_bracket",
  when($"normalized_user_id".startsWith("hot_user_"), lit("high")).otherwise(lit("low"))
)
```

In the mock data, `hot_user_*` users represent the intentionally skewed high-frequency users.

This turns the numeric score into an eligibility band:

```scala
.withColumn("eligibility_band", ...)
```

The values are:

```text
very_high
high
medium
low
```

This creates a numeric flag from the boolean:

```scala
.withColumn("eligible_user_flag", when($"is_eligible" === true, lit(1)).otherwise(lit(0)))
```

This removes duplicate user/bid rows if any exist:

```scala
.dropDuplicates("bid_request_id", "user_id_hash")
```

Before writing, the job repartitions by downstream join/split keys:

```scala
val output = converted.repartition($"user_id_hash", $"frequency_bracket")
```

The job prints basic validation metrics:

```scala
raw_matched_user_data_count
converted_eligible_user_data_count
eligible_user_count
high_frequency_count
low_frequency_count
```

Finally, the converted dataset is written here:

```scala
DatasetIO.writeConverted(output, config, "eligible_user_data")
```

Short version:

```text
Read raw matched_user_data
Clean user id
Create user and bid hashes
Create eligibility fields
Create high/low frequency bracket
Deduplicate
Repartition by downstream keys
Write converted eligible_user_data
```

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
  --class com.demo.emr.EligibleUserDataLogConverter \
  emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar \
  --bucket aigithub-emr-2026 \
  --base-prefix emr-migration-demo \
  --run-date 2026-05-21 \
  --hour 10 \
  --late-hour 11 \
  --output-mode overwrite
```

Observed:

```text
eligible_user_data_converter.raw_matched_user_data_count=20000
eligible_user_data_converter.converted_eligible_user_data_count=20000
eligible_user_data_converter.eligible_user_count=5000
eligible_user_data_converter.high_frequency_count=4000
eligible_user_data_converter.low_frequency_count=16000
```

Validation conclusion:

```text
PASS
```

The raw and converted row counts match for the tiny smoke dataset.

## Manual Before/After Sample Check

Use `spark-shell` to inspect a few raw matched-user records and converted eligible-user records.

```bash
spark-shell
```

Then run:

```scala
val rawMatchedPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/matched_user_data/year=2026/month=05/day=21/hour=10/"

val convertedEligiblePath =
  "s3://aigithub-emr-2026/emr-migration-demo/converted/eligible_user_data/year=2026/month=05/day=21/hour=10/"

val rawMatched = spark.read.parquet(rawMatchedPath)
val convertedEligible = spark.read.parquet(convertedEligiblePath)

val bidIdsToCheck = Array(
  "br_0",
  "br_1",
  "br_2",
  "br_3",
  "br_4"
)
```

Raw sample:

```scala
rawMatched
  .where($"bid_request_id".isin(bidIdsToCheck: _*))
  .select(
    "bid_request_id",
    "user_id",
    "user_id_hash",
    "segment_id",
    "is_eligible",
    "eligibility_score"
  )
  .orderBy("bid_request_id")
  .show(false)
```

Converted sample:

```scala
convertedEligible
  .where($"bid_request_id".isin(bidIdsToCheck: _*))
  .select(
    "bid_request_id",
    "bid_request_hash",
    "user_id",
    "normalized_user_id",
    "source_user_id_hash",
    "user_id_hash",
    "segment_id",
    "is_eligible",
    "eligible_user_flag",
    "eligibility_score",
    "eligibility_band",
    "frequency_bracket"
  )
  .orderBy("bid_request_id")
  .show(false)
```

For wide output, use vertical mode:

```scala
convertedEligible
  .where($"bid_request_id".isin(bidIdsToCheck: _*))
  .select(
    "bid_request_id",
    "bid_request_hash",
    "user_id",
    "normalized_user_id",
    "source_user_id_hash",
    "user_id_hash",
    "segment_id",
    "is_eligible",
    "eligible_user_flag",
    "eligibility_score",
    "eligibility_band",
    "frequency_bracket"
  )
  .orderBy("bid_request_id")
  .show(5, truncate = false, vertical = true)
```

Expected observations:

- `normalized_user_id` is added.
- `source_user_id_hash` preserves the raw hash.
- `bid_request_hash` is added.
- `eligible_user_flag` maps `true` to `1` and `false` to `0`.
- `eligibility_band` is added.
- `frequency_bracket` maps `hot_user_*` to `high`.
- `convertedEligible.count()` returns `20000`.

## Next Recommended Step

Step 10:

```text
com.demo.emr.BrbfJob
```

Implement the main BRBF before-DAG job.
