package com.demo.emr.common

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Udfs {
  val sha256String: UserDefinedFunction = udf { value: String =>
    if (value == null) {
      null
    } else {
      val digest = MessageDigest.getInstance("SHA-256")
      digest.digest(value.getBytes("UTF-8")).map("%02x".format(_)).mkString
    }
  }

  val stableUuidFromString: UserDefinedFunction = udf { value: String =>
    if (value == null) {
      null
    } else {
      val digest = MessageDigest.getInstance("MD5")
      val bytes = digest.digest(value.getBytes("UTF-8"))
      val buffer = ByteBuffer.wrap(bytes)
      new java.util.UUID(buffer.getLong, buffer.getLong).toString
    }
  }

  def roundTimestampToMinutes(minutes: Int): UserDefinedFunction = udf { timestamp: Timestamp =>
    if (timestamp == null) {
      null
    } else {
      val precisionSeconds = minutes.toLong * 60L
      val epochSeconds = timestamp.toInstant.getEpochSecond
      val roundedSeconds = Math.floorDiv(epochSeconds, precisionSeconds) * precisionSeconds
      Timestamp.from(Instant.ofEpochSecond(roundedSeconds))
    }
  }
}

