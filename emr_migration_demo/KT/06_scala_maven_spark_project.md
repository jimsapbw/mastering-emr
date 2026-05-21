# Step 06: Scala Maven Spark Project

## Purpose

Scaffold the Scala Spark Maven project that will later contain the three EMR step entry points.

This step proves that the cluster can:

- compile Scala with Maven
- package a Spark application JAR
- run a Scala Spark app with `spark-submit`
- read the raw S3 Parquet data created earlier

## Project Path

```text
emr_migration_demo/spark/
```

## Files Created

```text
emr_migration_demo/spark/pom.xml
emr_migration_demo/spark/src/main/scala/com/demo/emr/RawDataCountApp.scala
```

## Maven Settings

The project targets:

```text
Scala: 2.12.18
Spark: 3.5.6
Java target: 1.8
```

Spark dependencies are marked as `provided` because EMR supplies Spark at runtime.

## Build Command

```bash
cd emr_migration_demo/spark
mvn clean package
```

In this environment, Maven's default local repository under `/home/hadoop/.m2` was not writable from the sandbox. The successful build used a workspace-local Maven cache:

```bash
cd emr_migration_demo/spark
mvn -Dmaven.repo.local=/home/hadoop/mastering-emr/.m2/repository clean package
```

## Raw Data Count App

Entry point:

```text
com.demo.emr.RawDataCountApp
```

Purpose:

```text
Read the 8 raw datasets from S3 and print row counts.
```

Run command:

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

Expected tiny-scale counts:

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

## Observed Build Result

Maven build succeeded:

```text
BUILD SUCCESS
```

Created JAR:

```text
emr_migration_demo/spark/target/emr-migration-demo-0.1.0.jar
```

## Observed Run Result

The Scala Spark JAR ran successfully with `spark-submit` and read raw Parquet from S3.

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

The Maven/Scala/Spark JAR path is ready for the real three-step EMR application.
