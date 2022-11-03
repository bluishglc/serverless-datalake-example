package com.github.sdl

import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}

/**
 *  System constants, some are loaded from configuration file.
 *
 *  @author Laurence Geng
 */
object Constants {

  private val config: Config = ConfigFactory.load("sdl.conf")

  // spark related constants
  val DATA_BUCKET = config.getString("data.bucket")

  // partition key-value string format is: key1=value1,key2=value2,...
  val PT_KV_STRING_REGEX = """(\s*\w+\s*=\s*\w+\s*,?\s*)+"""

  val SPARK_FEATURE_HIVE = "HIVE"

  val SPARK_FEATURE_HUDI = "HUDI"

  val JOB_PROFILE_SQL = "sql"

  val HUDI_INSTANT_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  val PARTITION_PATTERN_BY_DAY = "year=@year@/month=@month@/day=@day@"

  val PARTITION_PATTERN_BY_MONTH = "year=@year@/month=@month@"

  val PARTITION_PATTERN_BY_YEAR = "year=@year@"

  val YEAR = "year"

  val MONTH = "month"

  val DAY = "day"

}