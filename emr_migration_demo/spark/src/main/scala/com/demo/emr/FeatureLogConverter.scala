package com.demo.emr

import com.demo.emr.common.{ArgsParser, AppConfig, DatasetIO, SparkSessionFactory, Udfs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FeatureLogConverter {
  def main(args: Array[String]): Unit = {
    val config = ArgsParser.parse(args)
    val spark = SparkSessionFactory.create("emr-migration-feature-log-converter")

    try {
      run(spark, config)
    } finally {
      spark.stop()
    }
  }

  private def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    val rawFeatureLog = DatasetIO.readRaw(spark, config, "feature_log")

    val converted = rawFeatureLog
      .withColumn("normalized_user_id", lower(trim($"user_id")))
      .withColumn("source_user_id_hash", $"user_id_hash")
      .withColumn("user_id_hash", Udfs.sha256String($"normalized_user_id"))
      .withColumn("user_uuid", Udfs.stableUuidFromString($"normalized_user_id"))
      .withColumn("contextual_join_key", Udfs.sha256String($"contextual_id".cast("string")))
      .withColumn("rounded_event_time", Udfs.roundTimestampToMinutes(5)($"event_timestamp"))
      .withColumn(
        "feature_value_bucket",
        when($"feature_value" >= 0.75, lit("very_high"))
          .when($"feature_value" >= 0.50, lit("high"))
          .when($"feature_value" >= 0.25, lit("medium"))
          .otherwise(lit("low"))
      )
      .dropDuplicates("feature_event_id")
      .select(
        $"feature_event_id",
        $"user_id",
        $"normalized_user_id",
        $"source_user_id_hash",
        $"user_id_hash",
        $"user_uuid",
        $"contextual_id",
        $"contextual_join_key",
        $"feature_name",
        $"feature_value",
        $"feature_value_bucket",
        $"event_timestamp",
        $"rounded_event_time",
        $"year",
        $"month",
        $"day",
        $"hour"
      )

    val output = converted.repartition($"user_id_hash", $"contextual_id")

    printMetric("raw_feature_log_count", rawFeatureLog.count())
    printMetric("converted_feature_log_count", converted.count())
    printMetric("distinct_user_hash_count", converted.select($"user_id_hash").distinct().count())
    printMetric("distinct_contextual_count", converted.select($"contextual_id").distinct().count())

    DatasetIO.writeConverted(output, config, "feature_log")
  }

  private def printMetric(name: String, value: Long): Unit = {
    println(s"feature_log_converter.$name=$value")
  }
}
