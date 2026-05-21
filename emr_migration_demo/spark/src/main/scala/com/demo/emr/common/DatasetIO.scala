package com.demo.emr.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object DatasetIO {
  def readRaw(
      spark: SparkSession,
      config: AppConfig,
      dataset: String,
      hour: Option[String] = None,
      hasHour: Boolean = true
  ): DataFrame = {
    spark.read.parquet(S3Paths.raw(config, dataset, hour, hasHour))
  }

  def readConverted(
      spark: SparkSession,
      config: AppConfig,
      dataset: String,
      hour: Option[String] = None,
      hasHour: Boolean = true
  ): DataFrame = {
    spark.read.parquet(S3Paths.converted(config, dataset, hour, hasHour))
  }

  def writeConverted(
      df: DataFrame,
      config: AppConfig,
      dataset: String,
      hour: Option[String] = None,
      hasHour: Boolean = true,
      partitionCount: Option[Int] = None
  ): Unit = {
    writeParquet(df, S3Paths.converted(config, dataset, hour, hasHour), config.outputMode, partitionCount)
  }

  def writeFinal(
      df: DataFrame,
      config: AppConfig,
      dataset: String,
      hour: Option[String] = None,
      hasHour: Boolean = true,
      partitionCount: Option[Int] = None
  ): Unit = {
    writeParquet(df, S3Paths.finalOutput(config, dataset, hour, hasHour), config.outputMode, partitionCount)
  }

  private def writeParquet(
      df: DataFrame,
      path: String,
      mode: String,
      partitionCount: Option[Int]
  ): Unit = {
    val output = partitionCount.map(df.repartition).getOrElse(df)

    output.write
      .mode(mode)
      .option("compression", "snappy")
      .parquet(path)
  }
}

