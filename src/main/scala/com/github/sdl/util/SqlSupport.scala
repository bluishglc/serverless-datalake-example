package com.github.sdl.util

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession

/**
 * SQL support class, add reading and executing sql files ability to spark via implicit conversion.
 *
 * @author Laurence Geng
 */
object SqlSupport {

	implicit class SparkSqlEnhancer(spark: SparkSession) {

		private val logger = new GlueLogger()

		def execSqlFile(sqlFilesOpt: Option[String], sqlParams: Option[String]) = {
			val sqlFiles = sqlFilesOpt.get
			spark
				.sparkContext
				.wholeTextFiles(sqlFiles, 1)
				.values
				.collect
				.mkString(";")
				.split(";")
				.filter(StringUtils.isNotBlank(_))
				.foreach {
					sql =>
						val sqlBuilder = sqlParams.foldLeft(sql) {
							(sql, params) =>
								params.split(",").foldLeft(sql) {
									(sql, kv) =>
										val Array(key, value) = kv.split("=")
										logger.info(s"Sql Param Key = $key, Sql Param Value = $value")
										sql.replace(s"@${key}@", value)
								}
						}
						logger.info(s"Sql to be executed: ${sqlBuilder}")
						spark.sql(sqlBuilder)
				}
		}
	}

}
