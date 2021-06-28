package com.github.sdl.util

import com.amazonaws.services.glue.log.GlueLogger
import com.github.sdl.Constants._

/**
 * A util class to provide some table path / view name resolving functions
 *
 * @Author Laurence Geng
 */
object TableUtil {

	private val logger = new GlueLogger()

	/**
	 * for a stg table, i.e. "stg.geo_taxi_zone", its path should be s3://$DATA_BUCKET/stg/geo/taxi_zone
	 * for a non-stg table, i.e. "ods.geo_taxi_zone", its path should be s3://$DATA_BUCKET/ods/geo_taxi_zone
	 */
	def resolveTablePath(tableNameOpt: Option[String]): String = {
		val tableName = tableNameOpt.get
		if (tableName.startsWith("stg.")) {
			val pattern = """[a-zA-Z0-9]+\.([a-zA-Z0-9]+)_(.+)$""".r
			val pattern(srcDatabase,srcTable) = tableName
			s"s3://$DATA_BUCKET/stg/$srcDatabase/$srcTable"
		}
		else {
			s"s3://$DATA_BUCKET/${tableName.replace('.','/')}"
		}
	}

	/**
	 * Converts a table name to a view name with default rule:
	 * 1. add "v_" prefix before table name
	 * 2. replace '.' to '_' in table name
	 * for example, the default view of table ods.geo_taxi_zone should be v_ods_geo_taxi_zone
	 */
	def toViewName(tableNameOpt: Option[String]): String = {
		s"v_${tableNameOpt.get.replace('.','_')}"
	}

}
