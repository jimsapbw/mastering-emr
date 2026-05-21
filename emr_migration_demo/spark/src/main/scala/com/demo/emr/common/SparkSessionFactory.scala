package com.demo.emr.common

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

