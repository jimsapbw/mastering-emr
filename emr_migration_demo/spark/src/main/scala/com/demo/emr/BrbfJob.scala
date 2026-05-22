package com.demo.emr

import com.demo.emr.common.{ArgsParser, AppConfig, DatasetIO, SparkSessionFactory, Udfs}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BrbfJob {
  private val SaltBuckets = 16

  def main(args: Array[String]): Unit = {
    val config = ArgsParser.parse(args)
    val spark = SparkSessionFactory.create("emr-migration-brbf-before-dag")

    try {
      run(spark, config)
    } finally {
      spark.stop()
    }
  }

  private def run(spark: SparkSession, config: AppConfig): Unit = {
    import spark.implicits._

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.default.parallelism", "200")

    val bids = prepareBids(DatasetIO.readRaw(spark, config, "bids"))
    val impressions = prepareImpressions(
      DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.hour)),
      DatasetIO.readRaw(spark, config, "impressions_feedback", Some(config.lateHour))
    )
    val contextual = prepareContextual(DatasetIO.readRaw(spark, config, "contextual"))
    val advertiser = prepareAdvertiser(DatasetIO.readRaw(spark, config, "advertiser", hasHour = false))
    val koaSettings = prepareKoaSettings(DatasetIO.readRaw(spark, config, "koa_settings", hasHour = false))
    val eligibleUserData = prepareEligibleUserData(DatasetIO.readConverted(spark, config, "eligible_user_data"))
    val featureLog = prepareFeatureLog(DatasetIO.readConverted(spark, config, "feature_log"))
    val sib = prepareSib(DatasetIO.readRaw(spark, config, "sib", hasHour = false))

    printMetric("source_bids_count", bids.count())
    printMetric("source_impressions_count", impressions.count())
    printMetric("source_contextual_count", contextual.count())
    printMetric("source_advertiser_count", advertiser.count())
    printMetric("source_koa_settings_count", koaSettings.count())
    printMetric("source_eligible_user_data_count", eligibleUserData.count())
    printMetric("source_feature_log_count", featureLog.count())
    printMetric("source_sib_count", sib.count())

    val joined = bids
      .join(impressions, Seq("bid_request_id"), "left")
      .join(contextual, Seq("contextual_id"), "left")
      .join(broadcast(advertiser), Seq("advertiser_id"), "left")
      .join(broadcast(koaSettings), Seq("advertiser_id", "campaign_id"), "left")
      .join(eligibleUserData, Seq("bid_request_id", "user_id_hash"), "left")
      .join(
        featureLog,
        bids("user_id_hash") === featureLog("feature_user_id_hash") &&
          bids("contextual_id") === featureLog("feature_contextual_id"),
        "left"
      )
      .join(sib, bids("user_id_hash") === sib("sib_user_id_hash"), "left")
      .withColumn("final_frequency_bracket", coalesce($"eligible_frequency_bracket", $"bid_frequency_bracket"))
      .withColumn("is_high_frequency", $"final_frequency_bracket" === lit("high"))
      .withColumn(
        "effective_bid_modifier",
        coalesce($"bid_modifier", lit(1.0d))
      )
      .withColumn(
        "adjusted_bid_price",
        $"bid_price" * $"effective_bid_modifier"
      )
      .withColumn(
        "quality_weighted_price",
        $"adjusted_bid_price" * $"event_quality_score"
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    printMetric("joined_row_count", joined.count())
    printMetric("joined_impression_match_count", joined.filter($"impression_id".isNotNull).count())
    printMetric("joined_eligible_user_match_count", joined.filter($"segment_id".isNotNull).count())
    printMetric("joined_feature_log_match_count", joined.filter($"feature_event_id".isNotNull).count())
    printMetric("joined_sib_match_count", joined.filter($"sib_group".isNotNull).count())
    printMetric("high_frequency_joined_count", joined.filter($"is_high_frequency").count())
    printMetric("low_frequency_joined_count", joined.filter(!$"is_high_frequency").count())

    val highFrequencyAgg = aggregateHighFrequency(joined.filter($"is_high_frequency"))
    val lowFrequencyAgg = aggregateLowFrequency(joined.filter(!$"is_high_frequency"))

    val finalOutput = highFrequencyAgg
      .unionByName(lowFrequencyAgg)
      .sort(
        $"advertiser_id",
        $"campaign_id",
        $"contextual_id",
        $"frequency_bracket",
        $"branch"
      )

    printMetric("final_output_count", finalOutput.count())

    DatasetIO.writeFinal(finalOutput, config, "brbf")
    joined.unpersist()
  }

  private def prepareBids(raw: DataFrame): DataFrame = {
    raw
      .withColumn("normalized_user_id", lower(trim(col("user_id"))))
      .withColumn("source_user_id_hash", col("user_id_hash"))
      .withColumn("user_id_hash", Udfs.sha256String(col("normalized_user_id")))
      .withColumn("bid_request_hash", Udfs.sha256String(col("bid_request_id")))
      .withColumn("contextual_join_key", Udfs.sha256String(col("contextual_id").cast("string")))
      .withColumn("rounded_bid_time", Udfs.roundTimestampToMinutes(5)(col("bid_timestamp")))
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
      .select(
        col("bid_request_id"),
        col("bid_request_hash"),
        col("user_id"),
        col("normalized_user_id"),
        col("source_user_id_hash"),
        col("user_id_hash"),
        col("advertiser_id"),
        col("campaign_id"),
        col("contextual_id"),
        col("contextual_join_key"),
        col("bid_timestamp"),
        col("rounded_bid_time"),
        col("bid_price"),
        col("bid_price_bucket"),
        col("device_type"),
        col("geo_id"),
        col("daily_user_freq"),
        col("bid_frequency_bracket"),
        col("event_quality_score"),
        col("campaign_grouping_key"),
        col("year"),
        col("month"),
        col("day"),
        col("hour")
      )
  }

  private def prepareImpressions(sameHour: DataFrame, lateHour: DataFrame): DataFrame = {
    sameHour
      .unionByName(lateHour)
      .dropDuplicates("bid_request_id")
      .select(
        col("bid_request_id"),
        col("impression_id"),
        col("feedback_timestamp"),
        col("is_billable"),
        col("clearing_price"),
        col("feedback_type")
      )
  }

  private def prepareContextual(raw: DataFrame): DataFrame = {
    raw.select(
      col("contextual_id"),
      col("domain"),
      col("content_category"),
      col("is_hot_context"),
      col("context_score")
    )
  }

  private def prepareAdvertiser(raw: DataFrame): DataFrame = {
    raw.select(
      col("advertiser_id"),
      col("advertiser_name"),
      col("vertical"),
      col("priority_tier"),
      col("is_active")
    )
  }

  private def prepareKoaSettings(raw: DataFrame): DataFrame = {
    raw.select(
      col("advertiser_id"),
      col("campaign_id"),
      col("koa_group"),
      col("bid_modifier"),
      col("policy_mode")
    )
  }

  private def prepareEligibleUserData(raw: DataFrame): DataFrame = {
    raw.select(
      col("bid_request_id"),
      col("user_id_hash"),
      col("segment_id"),
      col("is_eligible").as("eligible_user_is_eligible"),
      col("eligible_user_flag"),
      col("eligibility_score"),
      col("eligibility_band"),
      col("frequency_bracket").as("eligible_frequency_bracket")
    )
  }

  private def prepareFeatureLog(raw: DataFrame): DataFrame = {
    raw.select(
      col("feature_event_id"),
      col("user_id_hash").as("feature_user_id_hash"),
      col("contextual_id").as("feature_contextual_id"),
      col("feature_name"),
      col("feature_value"),
      col("feature_value_bucket"),
      col("rounded_event_time").as("feature_rounded_event_time")
    )
  }

  private def prepareSib(raw: DataFrame): DataFrame = {
    raw
      .dropDuplicates("user_id_hash")
      .select(
        col("user_id_hash").as("sib_user_id_hash"),
        col("sib_group"),
        col("sib_score"),
        col("sib_source")
      )
  }

  private def aggregateHighFrequency(df: DataFrame): DataFrame = {
    df
      .withColumn("salt", pmod(abs(hash(col("bid_request_id"))), lit(SaltBuckets)))
      .withColumn("salted_user_key", concat_ws("#", col("user_id_hash"), col("salt")))
      .repartition(col("salted_user_key"), col("contextual_id"))
      .groupBy(
        col("advertiser_id"),
        col("campaign_id"),
        col("contextual_id"),
        col("final_frequency_bracket").as("frequency_bracket"),
        col("content_category"),
        col("koa_group"),
        col("salt")
      )
      .agg(
        count(lit(1)).as("event_count"),
        countDistinct(col("bid_request_id")).as("bid_count"),
        count(col("impression_id")).as("impression_count"),
        sum(coalesce(col("eligible_user_flag"), lit(0))).as("eligible_user_count"),
        count(col("feature_event_id")).as("feature_event_count"),
        avg(col("bid_price")).as("avg_bid_price"),
        avg(col("clearing_price")).as("avg_clearing_price"),
        avg(col("quality_weighted_price")).as("avg_quality_weighted_price"),
        avg(col("sib_score")).as("avg_sib_score")
      )
      .withColumn("branch", lit("high_frequency_salted"))
      .selectFinalColumns
  }

  private def aggregateLowFrequency(df: DataFrame): DataFrame = {
    df
      .repartition(col("user_id_hash"), col("contextual_id"))
      .groupBy(
        col("advertiser_id"),
        col("campaign_id"),
        col("contextual_id"),
        col("final_frequency_bracket").as("frequency_bracket"),
        col("content_category"),
        col("koa_group")
      )
      .agg(
        count(lit(1)).as("event_count"),
        countDistinct(col("bid_request_id")).as("bid_count"),
        count(col("impression_id")).as("impression_count"),
        sum(coalesce(col("eligible_user_flag"), lit(0))).as("eligible_user_count"),
        count(col("feature_event_id")).as("feature_event_count"),
        avg(col("bid_price")).as("avg_bid_price"),
        avg(col("clearing_price")).as("avg_clearing_price"),
        avg(col("quality_weighted_price")).as("avg_quality_weighted_price"),
        avg(col("sib_score")).as("avg_sib_score")
      )
      .withColumn("salt", lit(null).cast("int"))
      .withColumn("branch", lit("low_frequency_hash"))
      .selectFinalColumns
  }

  private implicit class FinalColumnSelector(df: DataFrame) {
    def selectFinalColumns: DataFrame = {
      df.select(
        col("advertiser_id"),
        col("campaign_id"),
        col("contextual_id"),
        col("frequency_bracket"),
        col("content_category"),
        col("koa_group"),
        col("salt"),
        col("branch"),
        col("event_count"),
        col("bid_count"),
        col("impression_count"),
        col("eligible_user_count"),
        col("feature_event_count"),
        col("avg_bid_price"),
        col("avg_clearing_price"),
        col("avg_quality_weighted_price"),
        col("avg_sib_score")
      )
    }
  }

  private def printMetric(name: String, value: Long): Unit = {
    println(s"brbf_job.$name=$value")
  }
}
