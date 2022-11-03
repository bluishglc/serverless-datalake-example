package com.github.sdl

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.github.sdl.util.CsvSupport._
import com.github.sdl.util.HudiSupport._
import com.github.sdl.util.SqlSupport._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * A generic glue job class which can process data with sql and hudi,
 * Besides, you can also assign a pre-process operation: src-action and
 * a post-process operation: sink-action.
 *
 * @author Laurence Geng
 */
object GenericJob {

	private val logger = new GlueLogger

	private var args: Map[String, Option[String]] = _

	private var spark: SparkSession = _

	def main(sysArgs: Array[String]) {

		args = resolveArgs(sysArgs)
		spark = buildSpark()

		args("src-action") match {
			case Some(srcAction) => {
				srcAction match {
					case "load-csv-as-view" => spark.loadCsvAsView(args("src-table"))
					case _ => throw new RuntimeException(s"Unsupported src action: [ $srcAction ]")
				}
			}
			case _ =>
		}

		args("action") match {
			case Some(etl) => {
				etl match {
					case "sql" => {
						spark.execSqlFile(args("sql-files"), args("sql-params"))
					}
					case _ => throw new RuntimeException(s"Unsupported job action: [ $etl ]")
				}
			}
			case _ =>
		}

		args("sink-action") match {
			case Some(sinkAction) => {
				sinkAction match {
					case "save-view-to-hudi" => {
						spark.saveViewToHudi(args("sink-table"), args("hudi-record-key-field"), args("hudi-precombine-field"), args("hudi-partition-path-field"))
					}
					case _ => throw new RuntimeException(s"Unsupported sink action: [ $sinkAction ]")
				}
			}
			case _ =>
		}

		spark.close()

	}

	private def resolveArgs(sysArgs: Array[String]): Map[String, Option[String]] = {

		val args = mutable.LinkedHashMap[String, Option[String]](
			"JOB_NAME" -> None,
			"src-action" -> None,
			"src-table" -> None,
			"etl" -> None,
			"sql-files" -> None,
			"sql-params" -> None,
			"sink-action" -> None,
			"sink-table" -> None,
			"hudi-record-key-field" -> None,
			"hudi-precombine-field" -> None,
			"hudi-partition-path-field" -> None
		)

		sysArgs.sliding(2).foreach {
			case Array(l, r) if l.startsWith("--") && !r.startsWith("--") => {
				args.put(l.substring(2), Some(r))
			}
			case _ =>
		}

		logger.info(s"Resolved Args: ${args.mkString(", ")}")
		args.toMap
	}

	private def buildSpark(): SparkSession = {
		val conf = new SparkConf()
		conf.setAppName(args("JOB_NAME").get)
		conf.set("spark.logConf", "true")
		conf.set("hive.exec.dynamic.partition", "true")
		conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		conf.set("spark.sql.parser.quotedRegexColumnNames", "true")
		conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
		//  for hudi feature, it's up to if src or sink use hudi.
		val enableHudi = args("src-action").map(_ equals "load-hudi-as-view").getOrElse(false) ||
			               args("sink-action").map(_ equals "save-view-to-hudi").getOrElse(false)
		if (enableHudi) {
			conf.set("spark.sql.hive.convertMetastoreParquet", "false")
			conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		}

		val sparkContext = new SparkContext(conf)
		sparkContext.setLogLevel("INFO")
		new GlueContext(sparkContext).getSparkSession
	}
}
