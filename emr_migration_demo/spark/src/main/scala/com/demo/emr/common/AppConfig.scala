package com.demo.emr.common

final case class AppConfig(
    bucket: String = "aigithub-emr-2026",
    basePrefix: String = "emr-migration-demo",
    runDate: String = "2026-05-21",
    hour: String = "10",
    lateHour: String = "11",
    outputMode: String = "overwrite"
)

