# Reference: Simple Scala App To Shared Utility App

This note is for KT. It shows how the first simple Scala Spark app evolved into the shared-utility structure used by the EMR demo.

## Why This Matters

The first version was intentionally simple:

- one file
- argument parsing inside the app
- S3 path building inside the app
- Spark session creation inside the app
- direct Parquet reads inside the app

That is good for learning, but it becomes repetitive once we create multiple EMR steps.

The current version extracts reusable code into:

```text
com.demo.emr.common.AppConfig
com.demo.emr.common.ArgsParser
com.demo.emr.common.S3Paths
com.demo.emr.common.SparkSessionFactory
com.demo.emr.common.DatasetIO
com.demo.emr.common.Udfs
```

## Simple One-File Version

This is a simplified reference version of the original pattern.

```scala
package com.demo.emr

import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleRawDataCountApp {
  final case class Config(
      bucket: String = "aigithub-emr-2026",
      basePrefix: String = "emr-migration-demo",
      runDate: String = "2026-05-21",
      hour: String = "10",
      lateHour: String = "11"
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession
      .builder()
      .appName("emr-migration-raw-data-count")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val bids = readRaw(spark, config, "bids")
      val impressionsSame =
        readRaw(spark, config, "impressions_feedback", Some(config.hour))
      val impressionsLate =
        readRaw(spark, config, "impressions_feedback", Some(config.lateHour))
      val contextual = readRaw(spark, config, "contextual")
      val matchedUserData = readRaw(spark, config, "matched_user_data")
      val advertiser = readRaw(spark, config, "advertiser", hasHour = false)
      val koaSettings = readRaw(spark, config, "koa_settings", hasHour = false)
      val featureLog = readRaw(spark, config, "feature_log")
      val sib = readRaw(spark, config, "sib", hasHour = false)

      println("== RAW DATA COUNTS ==")
      printCount("bids", bids)
      printCount("impressions_feedback_same_hour", impressionsSame)
      printCount("impressions_feedback_late_hour", impressionsLate)
      println(s"counts.impressions_feedback_total=${impressionsSame.count() + impressionsLate.count()}")
      printCount("contextual", contextual)
      printCount("matched_user_data", matchedUserData)
      printCount("advertiser", advertiser)
      printCount("koa_settings", koaSettings)
      printCount("feature_log", featureLog)
      printCount("sib", sib)
    } finally {
      spark.stop()
    }
  }

  private def readRaw(
      spark: SparkSession,
      config: Config,
      dataset: String,
      hourOverride: Option[String] = None,
      hasHour: Boolean = true
  ): DataFrame = {
    spark.read.parquet(rawPath(config, dataset, hourOverride, hasHour))
  }

  private def rawPath(
      config: Config,
      dataset: String,
      hourOverride: Option[String],
      hasHour: Boolean
  ): String = {
    val Array(year, month, day) = config.runDate.split("-", 3)
    val dayPath =
      s"s3://${config.bucket}/${config.basePrefix}/raw/$dataset/year=$year/month=$month/day=$day"

    if (hasHour) {
      s"$dayPath/hour=${hourOverride.getOrElse(config.hour)}/"
    } else {
      s"$dayPath/"
    }
  }

  private def printCount(name: String, df: DataFrame): Unit = {
    println(s"counts.$name=${df.count()}")
  }

  private def parseArgs(args: Array[String]): Config = {
    val values = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    Config(
      bucket = values.getOrElse("bucket", "aigithub-emr-2026"),
      basePrefix = values.getOrElse("base-prefix", "emr-migration-demo"),
      runDate = values.getOrElse("run-date", "2026-05-21"),
      hour = values.getOrElse("hour", "10"),
      lateHour = values.getOrElse("late-hour", "11")
    )
  }
}
```

## Current Shared-Utility Version

The current app is smaller because common logic moved into reusable helpers.

```scala
package com.demo.emr

import com.demo.emr.common.{ArgsParser, AppConfig, DatasetIO, SparkSessionFactory}
import org.apache.spark.sql.DataFrame

object RawDataCountApp {
  def main(args: Array[String]): Unit = {
    val config = ArgsParser.parse(args)
    val spark = SparkSessionFactory.create("emr-migration-raw-data-count")

    try {
      run(spark, config)
    } finally {
      spark.stop()
    }
  }

  private def run(spark: org.apache.spark.sql.SparkSession, config: AppConfig): Unit = {
    val bids = DatasetIO.readRaw(spark, config, "bids")
    val impressionsSame =
      DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.hour))
    val impressionsLate =
      DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.lateHour))
    val contextual = DatasetIO.readRaw(spark, config, "contextual")
    val matchedUserData = DatasetIO.readRaw(spark, config, "matched_user_data")
    val advertiser = DatasetIO.readRaw(spark, config, "advertiser", hasHour = false)
    val koaSettings = DatasetIO.readRaw(spark, config, "koa_settings", hasHour = false)
    val featureLog = DatasetIO.readRaw(spark, config, "feature_log")
    val sib = DatasetIO.readRaw(spark, config, "sib", hasHour = false)

    println("== RAW DATA COUNTS ==")
    printCount("bids", bids)
    printCount("impressions_feedback_same_hour", impressionsSame)
    printCount("impressions_feedback_late_hour", impressionsLate)
    printMetric("impressions_feedback_total", impressionsSame.count() + impressionsLate.count())
    printCount("contextual", contextual)
    printCount("matched_user_data", matchedUserData)
    printCount("advertiser", advertiser)
    printCount("koa_settings", koaSettings)
    printCount("feature_log", featureLog)
    printCount("sib", sib)
  }

  private def printCount(name: String, df: DataFrame): Unit = {
    printMetric(name, df.count())
  }

  private def printMetric(name: String, value: Long): Unit = {
    println(s"counts.$name=$value")
  }
}
```

## What Moved Where

| Simple app responsibility | Shared utility |
|---|---|
| `Config` case class | `AppConfig.scala` |
| `parseArgs` function | `ArgsParser.scala` |
| `SparkSession.builder()` | `SparkSessionFactory.scala` |
| `rawPath` function | `S3Paths.scala` |
| `spark.read.parquet(...)` | `DatasetIO.scala` |
| later shared UDFs | `Udfs.scala` |

## Teaching Flow

For KT, explain it in this order:

1. A Scala Spark app starts from an `object` with a `main(args: Array[String])` method.
2. `spark-submit --class ...` runs that `main` method from the JAR.
3. The simple version keeps everything in one file, which is easy to understand.
4. Once we need 3 EMR steps, repeated logic becomes noisy.
5. Shared utilities keep each job focused on its transformation logic.
6. The refactor was verified by rebuilding the JAR and rerunning the same S3 count check.

## Key Lesson

The shared-utility version does not change the behavior. It changes the code organization so future jobs can reuse the same patterns safely.
