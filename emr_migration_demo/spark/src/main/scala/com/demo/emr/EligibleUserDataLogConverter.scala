package com.demo.emr

import com.demo.emr.common.{ArgsParser, AppConfig, DatasetIO, SparkSessionFactory, Udfs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object EligibleUserDataLogConverter {
  def main(args: Array[String]): Unit = {
    val config = ArgsParser.parse(args)
    val spark = SparkSessionFactory.create("emr-migration-eligible-user-data-converter")

    try {
      run(spark, config)
    } finally {
      spark.stop()
    }
  }

  private def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    val rawMatchedUserData = DatasetIO.readRaw(spark, config, "matched_user_data")

    val converted = rawMatchedUserData
      .withColumn("normalized_user_id", lower(trim($"user_id")))
      .withColumn("source_user_id_hash", $"user_id_hash")
      .withColumn("user_id_hash", Udfs.sha256String($"normalized_user_id"))
      .withColumn("user_uuid", Udfs.stableUuidFromString($"normalized_user_id"))
      .withColumn("bid_request_hash", Udfs.sha256String($"bid_request_id"))
      .withColumn(
        "frequency_bracket",
        when($"normalized_user_id".startsWith("hot_user_"), lit("high")).otherwise(lit("low"))
      )
      .withColumn(
        "eligibility_band",
        when($"eligibility_score" >= 75, lit("very_high"))
          .when($"eligibility_score" >= 50, lit("high"))
          .when($"eligibility_score" >= 25, lit("medium"))
          .otherwise(lit("low"))
      )
      .withColumn(
        "eligible_user_flag",
        when($"is_eligible" === true, lit(1)).otherwise(lit(0))
      )
      .dropDuplicates("bid_request_id", "user_id_hash")
      .select(
        $"bid_request_id",
        $"bid_request_hash",
        $"user_id",
        $"normalized_user_id",
        $"source_user_id_hash",
        $"user_id_hash",
        $"user_uuid",
        $"segment_id",
        $"is_eligible",
        $"eligible_user_flag",
        $"eligibility_score",
        $"eligibility_band",
        $"frequency_bracket",
        $"year",
        $"month",
        $"day",
        $"hour"
      )

    val output = converted.repartition($"user_id_hash", $"frequency_bracket")

    printMetric("raw_matched_user_data_count", rawMatchedUserData.count())
    printMetric("converted_eligible_user_data_count", converted.count())
    printMetric("eligible_user_count", converted.filter($"is_eligible" === true).count())
    printMetric("high_frequency_count", converted.filter($"frequency_bracket" === "high").count())
    printMetric("low_frequency_count", converted.filter($"frequency_bracket" === "low").count())

    DatasetIO.writeConverted(output, config, "eligible_user_data")
  }

  private def printMetric(name: String, value: Long): Unit = {
    println(s"eligible_user_data_converter.$name=$value")
  }
}
