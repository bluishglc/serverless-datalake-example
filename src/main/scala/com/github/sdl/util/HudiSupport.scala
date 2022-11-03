package com.github.sdl.util

import com.amazonaws.services.glue.log.GlueLogger
import com.github.sdl.util.TableUtil.{resolveTablePath, toViewName}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{MultiPartKeysValueExtractor, NonPartitionedExtractor}
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Hudi support class, add hudi dataset reading and writing ability to spark via implicit conversion.
 *
 * @author Laurence Geng
 */
object HudiSupport {

	implicit class SparkHudiEnhancer(spark: SparkSession) {

		private val logger = new GlueLogger()

		/**
		 * If latestCommit is provided, this is hudi incremental query, otherwise it is snapshot query.
		 * @deprecated
		 */
		def loadHudiAsView(tableNameOpt: Option[String]): Unit = {
			val latestCommit = HoodieDataSourceHelpers.latestCommit(FileSystem.get(spark.sparkContext.hadoopConfiguration), resolveTablePath(tableNameOpt))
			logger.info(s"latestCommit = $latestCommit")
			val df = spark.read.format("hudi")
				//.option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
				.option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
				.option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, latestCommit)
				// Optional, use glob pattern if querying certain partitions.
				// If data like users, this option is not applicable, because any user's info could be updated;
				// if data like orders, this option is useful, assume order data are freezed within 3 months,
				// this option's value should be last 3 months.
				// .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/year=2020/month=*/day=*")
				.load(resolveTablePath(tableNameOpt)) // For incremental query, pass in the root/base path of table
			logger.info(s"Table path [ ${resolveTablePath(tableNameOpt)} ]")
			logger.info(s"Total load [ ${df.count()} ] records!")
			df.createOrReplaceTempView(toViewName(tableNameOpt))
		}

		def saveViewToHudi(tableNameOpt: Option[String], hudiRecordKeyFieldOpt: Option[String], hudiPrecombineFieldOpt: Option[String], hudiPartitionPathFieldOpt: Option[String]): Unit = {
			val tableName = tableNameOpt.get
			val hudiRecordKeyField = hudiRecordKeyFieldOpt.get
			val hudiPrecombineField = hudiPrecombineFieldOpt.get
			val database = tableName.substring(0, tableName.indexOf('.'))
			val table = tableName.substring(tableName.indexOf('.') + 1)
			var hudiOptions = Map[String, String](
				HoodieWriteConfig.TABLE_NAME -> tableName,
				//HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP -> "1", // restrict commits count, default value is 24!
				//HoodieIndexConfig.INDEX_TYPE_PROP -> "GLOBAL_BLOOM", // make record key global unique!
				DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
				DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
				DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> hudiRecordKeyField,
				DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> hudiPrecombineField,
				// Register hudi dataset as hive table (sync meta data)
				DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
				DataSourceWriteOptions.HIVE_USE_JDBC_OPT_KEY -> "false", // For glue, it is required to disable sync via hive jdbc!
				DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> database,
				DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> table
			)

			hudiPartitionPathFieldOpt match {
				// for partitioned table...
				case Some(hudiPartitionPathField) => {
					hudiOptions += DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[ComplexKeyGenerator].getName
					hudiOptions += DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getName
					hudiOptions += DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY -> "true"
					hudiOptions += DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> hudiPartitionPathField
					hudiOptions += DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> hudiPartitionPathField
				}
				// for non-partitioned table...
				case _ => {
					hudiOptions += DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[NonpartitionedKeyGenerator].getName
					hudiOptions += DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[NonPartitionedExtractor].getName
				}
			}
			//Specify common DataSourceWriteOptions in the single hudiOptions variable
			logger.info(s"HUDI OPTIONS: ${hudiOptions.toString}")
			val tablePath = resolveTablePath(tableNameOpt)

			// always close existing metaStoreClient before save!!
			// so as hudi HiveSyncTool to creat a new metaStoreClient to sync metadata,
			// otherwise, an exception "Got runtime exception when hive syncing" will throw out!
			Hive.closeCurrent()

			// Write a DataFrame as a Hudi dataset
			try {
				spark
					.table(toViewName(tableNameOpt))
					.write
					.format("hudi")
					.options(hudiOptions)
					.mode(SaveMode.Append)
					.save(tablePath)
			} catch {
				case e: Throwable => logger.error(e.getCause.getCause.getMessage)
			}
		}
	}
}
