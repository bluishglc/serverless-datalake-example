package com.github.sdl.util

import com.amazonaws.services.glue.log.GlueLogger
import com.github.sdl.util.TableUtil.{resolveTablePath, toViewName}
import org.apache.spark.sql.SparkSession

/**
 * Csv support class, add reading csv dataset as table ability to spark via implicit conversion.
 * Actually, this class is used to fix a bug of Glue. With testing, it shows
 * if raw data is CSV files with header, with crawler, a table will be generated,
 * it works if read with GlueContext, however, if read with SparkContext, the header line will be treated
 * as normal records, even there is "skip header = true" option in table definition, it still does NOT work.
 *
 * So, the workaround is: reading as csv first, then attach schema from generated table!
 *
 * @author Laurence Geng
 */
object CsvSupport {

	implicit class SparkCsvEnhancer(spark: SparkSession) {

		private val logger = new GlueLogger()

		def loadCsvAsView(tableNameOpt: Option[String]): Unit = {
			val tableName = tableNameOpt.get
			val viewName = toViewName(tableNameOpt)
			val tablePath = resolveTablePath(tableNameOpt)
			val tableSchema = spark.table(tableName).schema

			spark
				.read
				.format("csv")
				.option("header", "true")
				.schema(tableSchema)
				.load(tablePath)
				.createOrReplaceTempView(viewName)
		}
	}

}
