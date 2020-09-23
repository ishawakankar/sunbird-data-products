
package org.sunbird.analytics.job.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, concat, count, lit}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobContext, JobDriver, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.sunbird.analytics.model
import org.sunbird.analytics.model.report.ETBMetricsModel.updateReportPath
import org.sunbird.analytics.model.report.VDNMetricsModel.{generateReport, getTenantInfo}
import org.sunbird.analytics.model.report.{ContentDetails, TenantInfo, TestContentdata, TextbookHierarchy, TextbookReportResult}
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}

case class TextbookInfoES(result: TextbookDataResult)
case class TextbookDataResult(content: List[TextbookDataSet], count: Int)
case class TextbookDataSet(channel: String, identifier: String, name: String,
                           board: Object, medium: Object, gradeLevel: Object, subject: Object, status: String)
case class TbResult(identifier: String,l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, contentType: String)
case class FinalReport(identifier: String,l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, contentType: String,slug:String,reportName:String)

object VDNMetricsV2 extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.report.VDNMetricsV2"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("VDNMetricsV2")
    JobLogger.start("VDNMetricsV2 Job Started executing", Option(Map("config" -> config, "model" -> "VDNMetricsV2")))

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)

    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")

    val sparkConf = sparkContext.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    //    val textBookInfo = getTextBooks()
    val textBookInfo = TextBookUtils.getTextBooks(configMap("modelParams").asInstanceOf[Map[String, AnyRef]], RestUtil)

    var finlData = List[TextbookReportResult]()
    var contentD = List[TestContentdata]()

    implicit val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val output=textBookInfo.map(textbook=>{
      val baseUrl = s"${AppConf.getConfig("hierarchy.search.api.url")}${AppConf.getConfig("hierarchy.search.api.path")}${textbook.identifier}"
      val finalUrl = if("Live".equals(textbook.status)) baseUrl else s"$baseUrl?mode=edit"
      val response = RestUtil.get[ContentDetails](finalUrl)
      if(null != response && "successful".equals(response.params.status)) {
        val data = response.result.content
        val dataTextbook = generateReport(List(data), List(), List(),data,List(),List("","0"))
        val textbookReport = dataTextbook._1
        val totalChapters = dataTextbook._3
        val report = textbookReport.map(f=>TextbookReportResult(data.identifier,f.l1identifier,f.board,f.medium,f.grade,f.subject,f.name,f.chapters,f.channel,totalChapters))
        val contentData = dataTextbook._2
        finlData = report.reverse ++ finlData
        contentD = contentData ++ contentD
      }
      (finlData,contentD)
    })
    JobLogger.log(s"VDNMetricsJob: textbook details ${output.length}", None, INFO)

    val reportRdd = sparkContext.parallelize(output.map(f=>f._1).flatten)
    val reportData =  reportRdd.map(f=>((f.identifier+","+f.l1identifier),f))
    val contentRdd = sparkContext.parallelize(output.flatMap(f => f._2))
    val contents = contentRdd.map(f=>((f.identifier+","+f.l1identifier),f))

    val tbReport = TextbookReportResult("","","","","","","","","","")
    val textbookReport = reportData.fullOuterJoin(contents).map(f=> (f._2._1.getOrElse(tbReport).channel,TbResult(f._2._1.getOrElse(tbReport).identifier,f._2._1.getOrElse(tbReport).l1identifier,
      f._2._1.getOrElse(tbReport).board,f._2._1.getOrElse(tbReport).medium,
      f._2._1.getOrElse(tbReport).grade,f._2._1.getOrElse(tbReport).subject,
      f._2._1.getOrElse(tbReport).name,f._2._1.getOrElse(tbReport).chapters,
      f._2._1.getOrElse(tbReport).channel,f._2._1.getOrElse(tbReport).totalChapters,
      f._2._2.getOrElse(TestContentdata("","","")).contentType)))
    JobLogger.log(s"VDNMetricsJob: joined reportRdd to contentRdd", None, INFO)

    val tenantInfo = getTenantInfo(RestUtil).map(f=>(f.id,f))
    JobLogger.log(s"VDNMetricsJob: getting tenant info", None, INFO)

    val testd = TbResult("","","","","","","","","","","")
    val reportds = textbookReport.fullOuterJoin(tenantInfo).map(f=>FinalReport(f._2._1.getOrElse(testd).identifier,f._2._1.getOrElse(testd).l1identifier,
      f._2._1.getOrElse(testd).board,f._2._1.getOrElse(testd).medium,f._2._1.getOrElse(testd).grade,
      f._2._1.getOrElse(testd).subject,f._2._1.getOrElse(testd).name,f._2._1.getOrElse(testd).chapters,
      f._2._1.getOrElse(testd).channel,f._2._1.getOrElse(testd).totalChapters,f._2._1.getOrElse(testd).contentType,
      f._2._2.getOrElse(TenantInfo("","Unknown")).slug,"vdn-report"))
    JobLogger.log(s"VDNMetricsJob: joined tenant info to textbook report", None, INFO)

    val report = reportds.toDF()
    report.persist(StorageLevel.MEMORY_ONLY)
    JobLogger.log(s"VDNMetricsJob: final report to df", None, INFO)

    //chapter level report
    val chapterReport = report.groupBy(report.drop("contentType").columns.map(col): _*)
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("l1identifier"))
      .drop("identifier","l1identifier","channel","id","totalChapters")
      .na.fill(0)
    JobLogger.log(s"VDNMetricsJob: extracted chapter level", None, INFO)

    //textbook level report
    val textbookRepo = report.groupBy(report.drop("l1identifier","chapters","contentType").columns.map(col): _*)
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("identifier"))
      .drop("identifier","channel","id")
      .na.fill(0)
    JobLogger.log(s"VDNMetricsJob: extracted textbook level", None, INFO)

    val reportconfigMap = configMap("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig")
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))

    reportConfig.output.map { f =>
      val reportConf = reportconfigMap.asInstanceOf[Map[String, AnyRef]]
      val mergeConf = reportConf.getOrElse("mergeConfig", Map()).asInstanceOf[Map[String,AnyRef]]

      var reportMap = updateReportName(mergeConf, reportConf, "ChapterLevel.csv")
      CourseUtils.postDataToBlob(chapterReport,f,configMap("modelParams").asInstanceOf[Map[String, AnyRef]].updated("reportConfig",reportMap))

      reportMap = updateReportName(mergeConf, reportConf, "TextbookLevel.csv")
      CourseUtils.postDataToBlob(textbookRepo,f,configMap("modelParams").asInstanceOf[Map[String, AnyRef]].updated("reportConfig",reportMap))
    }

    report.unpersist(true)
  }

  def updateReportName(mergeConf: Map[String,AnyRef], reportConfig: Map[String,AnyRef], reportPath: String): Map[String,AnyRef] = {
    val mergeMap = mergeConf map {
      case ("reportPath","vdn-report.csv") => "reportPath" -> reportPath
      case x => x
    }
    if(mergeMap.nonEmpty) reportConfig.updated("mergeConfig",mergeMap) else reportConfig
  }

  def getTextBooks(): List[TextbookDataSet] = {
    val url = Constants.COMPOSITE_SEARCH_URL

    val request = s"""{
                     |	"request": {
                     |		"filters": {
                     |           "contentType": "Textbook"
                     |		},
                     |		"sort_by": {
                     |			"createdOn": "desc"
                     |		},
                     |		"limit": 5
                     |	}
                     |}""".stripMargin
    val response = RestUtil.post[TextbookInfoES](url, request)
    if(null != response && response.result.count!=0) {
      response.result.content
    } else List[TextbookDataSet]()
  }

}
