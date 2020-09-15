
package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.analytics.model.report.VDNMetricsModel

object VDNMetricsJob extends optional.Application with IJob {

  implicit val className = "org.sunbird.analytics.job.report.VDNMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, VDNMetricsModel)
    JobLogger.log("Job Completed.")
  }

}
//package org.sunbird.analytics.job.report
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.apache.spark.sql.functions.lit
//import org.ekstep.analytics.framework.Level.INFO
//import org.ekstep.analytics.framework.util.DatasetUtil.extensions
//import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
//import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobContext, JobDriver, StorageConfig}
//import org.ekstep.analytics.model.ReportConfig
//import org.sunbird.analytics.util.TextBookUtils
//import org.sunbird.cloud.storage.conf.AppConf
//
//case class ContentHierarchy(identifier: String, hierarchy: String)
//case class TextbookHierarchy(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
//                             name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
//                             depth: Int, createdOn: String, children: Option[List[TextbookHierarchy]], index: Int, parent: String)
//
//case class TextbookReport(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String)
//case class ContentData(contentType: String, count: Int)
//case class TestContentdata(identifier: String, l1identifier: String, contentType: String)
//
//case class TextbookReportResult(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, totalChapters: String)
//
//object VDNMetricsJob extends optional.Application with IJob with BaseReportsJob {
//
//  implicit val className = "org.sunbird.analytics.job.report.VDNMetricsJob"
//  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
//
//  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
//    JobLogger.init("CourseMetricsJob")
//    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> "VDNMetrics")))
//
//    val jobConfig = JSONUtils.deserialize[JobConfig](config)
////val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
////    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
//
//    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)
//    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
//    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
//
//    execute(jobConfig)
//
//  }
//
//  private def execute(config: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
//    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
//    val sparkConf = sc.getConf
//      .set("es.write.operation", "upsert")
//      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
////val sparkConf = new SparkConf().setAppName("AnalyticsTestSuite").set("spark.default.parallelism", "2");
////        sparkConf.set("spark.sql.shuffle.partitions", "2")
////        sparkConf.setMaster("local[*]")
////        sparkConf.set("spark.driver.memory", "1g")
////        sparkConf.set("spark.memory.fraction", "0.3")
////        sparkConf.set("spark.memory.storageFraction", "0.5")
////        sparkConf.set("spark.cassandra.connection.host", "localhost")
////        sparkConf.set("spark.cassandra.connection.port", "9042")
////        sparkConf.set("es.nodes", "http://localhost")
//
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    val storageConfig = getStorageConfig("dev-data-store", "vdn-reports")
//    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
//
//    val tableData=spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
//      .select("identifier","hierarchy")
//
//    JobLogger.log(s"VDNMetricsJob: Processing dataframe", None, INFO)
//
//    var finlData = List[TextbookReportResult]()
//    var contentD = List[TestContentdata]()
//
//    val output=tableData.rdd.map(row => {
////      val hierarchy = JSONUtils.deserialize[ContentHierarchy](row)
//      val hierarchy = JSONUtils.deserialize[TextbookHierarchy](row.getString(1))
//      if(hierarchy.contentType!=null && hierarchy.contentType.getOrElse("").equalsIgnoreCase("Textbook")) {
//        val dataTextbook = generateReport(List(hierarchy), List(), List(),hierarchy,List(),List("","0"))
//
//        val textbookReport = dataTextbook._1
//        val totalChapters = dataTextbook._3
//        val report = textbookReport.map(f=>TextbookReportResult(f.identifier,f.board,f.medium,f.grade,f.subject,f.name,f.chapters,totalChapters))
//        val contentData = dataTextbook._2
//        finlData = report++finlData
//        contentD = contentData++contentD
//      }
//      (finlData,contentD)
//    })
//
//
////    JobLogger.log(s"VDNMetricsJob: output size: ${output.length}", None, INFO)
////
////    val reportData=output.map(f=>f._1).flatten.toDF()
////    val contents = output.map(f=>f._2).flatten.toDF()
////
////    JobLogger.log(s"VDNMetricsJob: Flattening reports", None, INFO)
////
////    val df = reportData.join(contents,Seq("identifier"),"left_outer")
////      .withColumn("slug",lit("unknown"))
////      .withColumn("reportName", lit("content-data"))
////
////    JobLogger.log(s"VDNMetricsJob: records stats before cloud upload: No of records: ${df.count()}", None, INFO)
//////    JobLogger.log(s"VDNMetricsJob: reportconfig: ${reportConfig.output}", None, INFO)
////
////
////
////    df.saveToBlobStore(storageConfig, "csv", "druid/"+"vdn-reports", Option(Map("header" -> "true")), None)
//
//    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> "VDNMetrics")))
//    fc.closeContext()
//  }
//
//  def prepareReports(spark: SparkSession, storageConfig: StorageConfig, config: JobConfig)(implicit fc: FrameworkContext): Unit = {
//
//  }
//
//  def generateReport(data: List[TextbookHierarchy], prevData: List[TextbookReport], newData: List[TextbookHierarchy],textbookInfo: TextbookHierarchy, contentInfo: List[TestContentdata], chapterInfo: List[String]): (List[TextbookReport],List[TestContentdata],String) = {
//    var textbookReport = prevData
//    var contentData = contentInfo
//    var l1identifier = chapterInfo(0)
//    var totalChapters = chapterInfo(1)
//    var textbook = List[TextbookHierarchy]()
//
//    data.map(units=> {
//      val children = units.children
//      if(units.depth==1) {
//        textbook = units :: newData
//        val contentType = units.contentType.getOrElse("")
//        l1identifier = units.identifier
//        val grade = TextBookUtils.getString(textbookInfo.gradeLevel)
//        val report = TextbookReport(l1identifier,textbookInfo.board,TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name)
//        totalChapters = (totalChapters.toInt+1).toString
//        textbookReport = report :: textbookReport
//      }
//
//      if(units.depth!=0 && units.contentType.getOrElse("").nonEmpty) {
//        contentData = TestContentdata(textbookInfo.identifier,l1identifier, units.contentType.get) :: contentData
//      }
//
//      if(children.isDefined) {
//        val textbookReportData = generateReport(children.get, textbookReport, textbook,textbookInfo, contentData,List(l1identifier,totalChapters))
//        textbookReport = textbookReportData._1
//        contentData = textbookReportData._2
//        totalChapters = textbookReportData._3
//      }
//    })
//
//
//    (textbookReport,contentData,totalChapters)
//  }
//
//}
