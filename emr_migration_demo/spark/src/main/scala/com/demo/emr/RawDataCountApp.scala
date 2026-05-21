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
    val impressionsSame = DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.hour))
    val impressionsLate = DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.lateHour))
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
