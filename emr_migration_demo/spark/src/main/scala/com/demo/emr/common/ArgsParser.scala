package com.demo.emr.common

object ArgsParser {
  def parse(args: Array[String]): AppConfig = {
    val values = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    AppConfig(
      bucket = values.getOrElse("bucket", "aigithub-emr-2026"),
      basePrefix = values.getOrElse("base-prefix", "emr-migration-demo"),
      runDate = values.getOrElse("run-date", "2026-05-21"),
      hour = values.getOrElse("hour", "10"),
      lateHour = values.getOrElse("late-hour", "11"),
      outputMode = values.getOrElse("output-mode", "overwrite")
    )
  }
}

