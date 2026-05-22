# Step 08: Feature Log Converter

## Purpose

Implement the first real EMR pipeline step:

```text
com.demo.emr.FeatureLogConverter
```

This step reads raw feature-log Parquet, applies UDF-style conversion logic, deduplicates feature events, repartitions by the downstream join keys, and writes the converted feature-log dataset.

This step also contributes to the broader EMR baseline problem statement:

```text
emr_migration_demo/KT/emr_baseline_problem_statement.md
```

## Source

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/feature_log/year=2026/month=05/day=21/hour=10/
```

## Output

```text
s3://aigithub-emr-2026/emr-migration-demo/converted/feature_log/year=2026/month=05/day=21/hour=10/
```

## Scala Entry Point

Added:

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/FeatureLogConverter.scala
```

The converter:

- reads `raw/feature_log`
- normalizes `user_id`
- preserves the source hash as `source_user_id_hash`
- recomputes `user_id_hash` with the shared SHA-256 UDF
- creates `user_uuid` with the shared stable UUID UDF
- creates `contextual_join_key`
- rounds `event_timestamp` into `rounded_event_time`
- creates `feature_value_bucket`
- drops duplicate `feature_event_id` rows
- repartitions by `user_id_hash` and `contextual_id`
- writes Snappy Parquet to `converted/feature_log`

## Code Walkthrough

`FeatureLogConverter` is the first real Spark pipeline step. Its job is to take the raw `feature_log` dataset, prepare stable join keys and time buckets, and write a converted feature-log dataset for the later BRBF job.

The object itself is the application entry point:

```scala
object FeatureLogConverter
```

When this class is passed to `spark-submit` with:

```bash
--class com.demo.emr.FeatureLogConverter
```

Spark starts from this object's `main` method.

The `main` method does three setup tasks:

```scala
val config = ArgsParser.parse(args)
val spark = SparkSessionFactory.create("emr-migration-feature-log-converter")
run(spark, config)
```

In simple terms:

- `ArgsParser` reads command-line options like `--bucket`, `--run-date`, and `--hour`.
- `SparkSessionFactory` creates the Spark session.
- `run` contains the actual feature-log conversion logic.

The raw feature log is read here:

```scala
val rawFeatureLog = DatasetIO.readRaw(spark, config, "feature_log")
```

For the current tiny run, that points to:

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/feature_log/year=2026/month=05/day=21/hour=10/
```

The main transformation starts from:

```scala
val converted = rawFeatureLog
```

Then the job adds conversion columns.

This cleans the user id:

```scala
.withColumn("normalized_user_id", lower(trim($"user_id")))
```

It lowercases and trims `user_id` so the downstream hash is stable.

This keeps the original raw hash, then recomputes the canonical hash:

```scala
.withColumn("source_user_id_hash", $"user_id_hash")
.withColumn("user_id_hash", Udfs.sha256String($"normalized_user_id"))
```

`source_user_id_hash` is useful for comparison/debugging. The new `user_id_hash` is the join key the rest of the Scala pipeline should use.

This creates a stable UUID-style value from the user id:

```scala
.withColumn("user_uuid", Udfs.stableUuidFromString($"normalized_user_id"))
```

This creates a hashed contextual join key:

```scala
.withColumn("contextual_join_key", Udfs.sha256String($"contextual_id".cast("string")))
```

This rounds event timestamps into five-minute buckets:

```scala
.withColumn("rounded_event_time", Udfs.roundTimestampToMinutes(5)($"event_timestamp"))
```

This turns a numeric feature value into a simple label:

```scala
.withColumn("feature_value_bucket", ...)
```

The bucket values are:

```text
very_high
high
medium
low
```

This removes duplicate feature events:

```scala
.dropDuplicates("feature_event_id")
```

The final `.select(...)` chooses the output columns and keeps them in a predictable order.

Before writing, the job repartitions the converted data:

```scala
val output = converted.repartition($"user_id_hash", $"contextual_id")
```

This groups data by the keys that the later BRBF job will join on. It is intentionally part of the EMR baseline because it gives us a visible repartition/shuffle behavior to compare later.

The job prints basic validation metrics:

```scala
printMetric("raw_feature_log_count", rawFeatureLog.count())
printMetric("converted_feature_log_count", converted.count())
printMetric("distinct_user_hash_count", converted.select($"user_id_hash").distinct().count())
printMetric("distinct_contextual_count", converted.select($"contextual_id").distinct().count())
```

Finally, the converted data is written here:

```scala
DatasetIO.writeConverted(output, config, "feature_log")
```

For the current tiny run, that writes to:

```text
s3://aigithub-emr-2026/emr-migration-demo/converted/feature_log/year=2026/month=05/day=21/hour=10/
```

Short version:

```text
Read raw feature_log
Clean user id
Create hash and UUID join keys
Round event time
Bucket feature values
Deduplicate feature events
Repartition by downstream join keys
Write converted feature_log
```

## Build Command

Maven was not present in the new shell, so it was installed again:

```bash
sudo dnf install -y maven
```

The default Maven local repo path was not writable in this session, so the build used `/mnt/tmp`:

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
  --class com.demo.emr.FeatureLogConverter \
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
feature_log_converter.raw_feature_log_count=50000
feature_log_converter.converted_feature_log_count=50000
feature_log_converter.distinct_user_hash_count=40200
feature_log_converter.distinct_contextual_count=13035
```

Validation conclusion:

```text
PASS
```

The raw and converted row counts match for the tiny smoke dataset.

## Manual Before/After Sample Check

Use `spark-shell` to inspect a few raw and converted feature-log records side by side.

```bash
spark-shell
```

Then run:

```scala
val rawPath =
  "s3://aigithub-emr-2026/emr-migration-demo/raw/feature_log/year=2026/month=05/day=21/hour=10/"

val convertedPath =
  "s3://aigithub-emr-2026/emr-migration-demo/converted/feature_log/year=2026/month=05/day=21/hour=10/"

val raw = spark.read.parquet(rawPath)
val converted = spark.read.parquet(convertedPath)

val idsToCheck = Array(
  "feature_event_0",
  "feature_event_1",
  "feature_event_10",
  "feature_event_100",
  "feature_event_1000"
)
```

Raw sample:

```scala
raw
  .where($"feature_event_id".isin(idsToCheck: _*))
  .select(
    "feature_event_id",
    "user_id",
    "user_id_hash",
    "contextual_id",
    "feature_value",
    "event_timestamp"
  )
  .orderBy("feature_event_id")
  .show(false)
```

Converted sample:

```scala
converted
  .where($"feature_event_id".isin(idsToCheck: _*))
  .select(
    "feature_event_id",
    "user_id",
    "normalized_user_id",
    "source_user_id_hash",
    "user_id_hash",
    "contextual_id",
    "feature_value",
    "feature_value_bucket",
    "event_timestamp",
    "rounded_event_time"
  )
  .orderBy("feature_event_id")
  .show(false)
```

Expected observations:

- `normalized_user_id` is added.
- `source_user_id_hash` preserves the raw hash.
- `feature_value_bucket` is added.
- `rounded_event_time` rounds timestamps down to five-minute buckets.
- `converted.count()` returns `50000`.

## Notes

The Spark run required elevated local execution because Spark needs local networking for the driver/executor services.

A direct `aws s3 ls` check from this shell failed because the AWS CLI could not locate credentials, but Spark was able to read and write the S3 paths through the runtime environment used by `spark-submit`.

## Next Recommended Step

Step 09:

```text
com.demo.emr.EligibleUserDataLogConverter
```

Implement the converter for `raw/matched_user_data` into `converted/eligible_user_data`.
