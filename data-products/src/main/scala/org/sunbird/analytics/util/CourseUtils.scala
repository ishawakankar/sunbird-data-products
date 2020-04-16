package org.sunbird.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.ekstep.analytics.model.{MergeFiles, MergeScriptConfig, OutputConfig, ReportConfig}
import org.sunbird.cloud.storage.conf.AppConf

//Getting live courses from compositesearch
case class CourseDetails(result: Result)
case class Result(content: List[CourseInfo])
case class CourseInfo(channel: String, identifier: String, name: String)

trait CourseReport {
  def getCourse(config: Map[String, AnyRef])(sc: SparkContext): DataFrame
  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame
  def getCourseBatchDetails(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame
  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame
}

object CourseUtils {

  implicit val className: String = "org.sunbird.analytics.util.CourseUtils"

  def getCourse(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = RestUtil.post[CourseDetails](apiURL, request).result.content
    val resRDD = sc.parallelize(response)
    resRDD.toDF("channel", "identifier", "courseName")
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def getCourseBatchDetails(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdCoursesKeyspace = Constants.SUNBIRD_COURSES_KEY_SPACE
    loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .select(
        col("courseid").as("courseId"),
        col("batchid").as("batchId"),
        col("name").as("batchName"),
        col("status").as("status")
      )
  }

  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace)).select("slug","id")
  }

  def postDataToBlob(data: DataFrame, outputConfig: OutputConfig, config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val key = config.getOrElse("key", null).asInstanceOf[String]

    val fieldsList = data.columns
    val dimsLabels = labelsLookup.filter(x => outputConfig.dims.contains(x._1)).values.toList
    val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*).na.fill("unknown")
    val reportFinalId = if (outputConfig.label.nonEmpty && outputConfig.label.get.nonEmpty) reportConfig.id + "/" + outputConfig.label.get else reportConfig.id
    val finalDf = renamedDf.na.replace("Status", Map("0"->BatchStatus(0).toString, "1"->BatchStatus(1).toString, "2"->BatchStatus(2).toString))
    finalDf.show(false)
    saveReport(finalDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> outputConfig.fileParameters), reportConfig)
  }

  def saveReport(data: DataFrame, config: Map[String, AnyRef], reportConfig: ReportConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val storageConfig = StorageConfig(config.getOrElse("store", "local").toString, config.getOrElse("container", "test-container").toString, config.getOrElse("filePath", "/tmp/druid-reports").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val format = config.getOrElse("format", "csv").asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.getOrElse("reportId", "").asInstanceOf[String]
    val fileParameters = config.getOrElse("fileParameters", List("")).asInstanceOf[List[String]]
    val dims = config.getOrElse("folderPrefix", List()).asInstanceOf[List[String]]
    val mergeConfig = reportConfig.mergeConfig
   val deltaFiles = if (dims.nonEmpty) {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(dims))
    } else {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), None)
    }
    println("merge config ",mergeConfig)
    if(mergeConfig.nonEmpty) {
      val mergeConf = mergeConfig.get
      println(mergeConf)
      println()
      println()
      println()
      val reportPath = mergeConf.reportPath
      val fileList = getDeltaFileList(deltaFiles,reportId,reportPath,storageConfig)
      val mergeScriptConfig = MergeScriptConfig(reportId, mergeConf.frequency, mergeConf.basePath, mergeConf.rollup,
        mergeConf.rollupAge, mergeConf.rollupCol, mergeConf.rollupRange, MergeFiles(fileList, List("Date")))
      mergeReport(mergeScriptConfig)
    } else {
      JobLogger.log(s"Merge report is not configured, hence skipping that step", None, INFO)
    }
  }

  def getDeltaFileList(deltaFiles: List[String], reportId: String, reportPath: String, storageConfig: StorageConfig): List[Map[String, String]] = {
    if("content_progress_metrics".equals(reportId) || "etb_metrics".equals(reportId)) {
      deltaFiles.map{f =>
        val reportPrefix = f.split(reportId)(1)
        Map("reportPath" -> reportPrefix, "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
      }
    } else {
      deltaFiles.map{f =>
        val reportPrefix = f.substring(0, f.lastIndexOf("/")).split(reportId)(1)
        Map("reportPath" -> (reportPrefix + "/" + reportPath), "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
      }
    }
  }

  def mergeReport(mergeConfig: MergeScriptConfig, virtualEnvDir: Option[String] = Option("/mount/venv")): Unit = {
    val mergeConfigStr = JSONUtils.serialize(mergeConfig)
    println("merge config: " + mergeConfigStr)
    val mergeReportCommand = Seq("bash", "-c",
      s"source ${virtualEnvDir.get}/bin/activate; " +
        s"dataproducts report_merger --report_config='$mergeConfigStr'")
    JobLogger.log(s"Merge report script command:: $mergeReportCommand", None, INFO)
    val mergeReportExitCode = ScriptDispatcher.dispatch(mergeReportCommand)
    if (mergeReportExitCode == 0) {
      JobLogger.log(s"Merge report script::Success", None, INFO)
    } else {
      JobLogger.log(s"Merge report script failed with exit code $mergeReportExitCode", None, ERROR)
      throw new Exception(s"Merge report script failed with exit code $mergeReportExitCode")
    }
  }
}