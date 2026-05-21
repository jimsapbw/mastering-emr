package com.demo.emr.common

object S3Paths {
  def raw(config: AppConfig, dataset: String, hour: Option[String] = None, hasHour: Boolean = true): String = {
    partitionPath(config, s"raw/$dataset", hour, hasHour)
  }

  def converted(config: AppConfig, dataset: String, hour: Option[String] = None, hasHour: Boolean = true): String = {
    partitionPath(config, s"converted/$dataset", hour, hasHour)
  }

  def finalOutput(config: AppConfig, dataset: String, hour: Option[String] = None, hasHour: Boolean = true): String = {
    partitionPath(config, s"final/$dataset", hour, hasHour)
  }

  def validation(config: AppConfig): String = {
    s"s3://${config.bucket}/${config.basePrefix}/validation/run_date=${config.runDate}/hour=${config.hour}/"
  }

  private def partitionPath(
      config: AppConfig,
      relativePrefix: String,
      hour: Option[String],
      hasHour: Boolean
  ): String = {
    val Array(year, month, day) = config.runDate.split("-", 3)
    val dayPath =
      s"s3://${config.bucket}/${config.basePrefix}/$relativePrefix/year=$year/month=$month/day=$day"

    if (hasHour) {
      s"$dayPath/hour=${hour.getOrElse(config.hour)}/"
    } else {
      s"$dayPath/"
    }
  }
}

