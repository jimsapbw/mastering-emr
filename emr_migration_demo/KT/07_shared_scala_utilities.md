# Step 07: Shared Scala Utilities

## Purpose

Add reusable Scala helpers before implementing the three EMR steps.

The goal is to keep each Spark entry point focused on business logic instead of repeating:

- argument parsing
- Spark session creation
- S3 path construction
- raw/converted/final Parquet reads and writes
- common UDF definitions

## Files Added

```text
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/AppConfig.scala
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/ArgsParser.scala
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/S3Paths.scala
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/SparkSessionFactory.scala
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/DatasetIO.scala
emr_migration_demo/spark/src/main/scala/com/demo/emr/common/Udfs.scala
```

## Utility Responsibilities

### AppConfig

Holds shared runtime settings:

```text
bucket
basePrefix
runDate
hour
lateHour
outputMode
```

### ArgsParser

Parses command-line arguments like:

```text
--bucket aigithub-emr-2026
--base-prefix emr-migration-demo
--run-date 2026-05-21
--hour 10
--late-hour 11
--output-mode overwrite
```

### S3Paths

Builds partition-aware S3 paths for:

```text
raw/
converted/
final/
validation/
```

Example:

```text
s3://aigithub-emr-2026/emr-migration-demo/raw/bids/year=2026/month=05/day=21/hour=10/
```

### SparkSessionFactory

Creates a named `SparkSession` and sets Spark logs to `WARN`.

### DatasetIO

Provides common Parquet readers and writers:

```text
readRaw
readConverted
writeConverted
writeFinal
```

Writers use Snappy-compressed Parquet.

### Udfs

Defines UDF helpers for the later before-DAG workload:

```text
sha256String
stableUuidFromString
roundTimestampToMinutes
```

These are intentionally available because the EMR baseline should include UDF-style transformations in the critical path.

## RawDataCountApp Refactor

`RawDataCountApp` was refactored to use:

```text
ArgsParser
SparkSessionFactory
DatasetIO
AppConfig
```

This proves the shared utilities work before they are used by the real EMR steps.

## Build Command

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/home/hadoop/mastering-emr/.m2/repository clean package
```

Observed:

```text
BUILD SUCCESS
```

## Run Command

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

Observed counts:

```text
counts.bids=100000
counts.impressions_feedback_same_hour=59500
counts.impressions_feedback_late_hour=10500
counts.impressions_feedback_total=70000
counts.contextual=20000
counts.matched_user_data=20000
counts.advertiser=1000
counts.koa_settings=1000
counts.feature_log=50000
counts.sib=20000
```

Validation conclusion:

```text
PASS
```

The refactor preserved behavior and the Scala utility layer is ready for the three EMR step implementations.
