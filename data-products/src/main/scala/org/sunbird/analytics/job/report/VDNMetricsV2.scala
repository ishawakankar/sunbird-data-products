
package org.sunbird.analytics.job.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobContext, JobDriver, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.sunbird.analytics.model.report.VDNMetricsModel.{generateReport, getTenantInfo}
import org.sunbird.analytics.model.report.{TestContentdata, TextbookHierarchy, TextbookReportResult}
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}
import org.sunbird.cloud.storage.conf.AppConf

case class TextbookInfoES(result: TextbookDataResult)
case class TextbookDataResult(content: List[TextbookDataSet], count: Int)
case class TextbookDataSet(channel: String, identifier: String, name: String,
                        board: Object, medium: Object, gradeLevel: Object, subject: Object, status: String)

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
//    val textBookInfo = TextBookUtils.getTextBooks(configMap, RestUtil)

    val textBookInfo = getTextBooks()
    var finlData = List[TextbookReportResult]()
    var contentD = List[TestContentdata]()

    implicit val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val output=textBookInfo.map(textbook=>{
      val data=spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
        .select("identifier","hierarchy")
        .where(col("identifier") === textbook.identifier)
      if(data.count()>0) {
        val hierarchy = JSONUtils.deserialize[TextbookHierarchy](data.first().getString(1))
        val dataTextbook = generateReport(List(hierarchy), List(), List(),hierarchy,List(),List("","0"))
        val textbookReport = dataTextbook._1
        val totalChapters = dataTextbook._3
        val report = textbookReport.map(f=>TextbookReportResult(f.identifier,f.board,f.medium,f.grade,f.subject,f.name,f.chapters,f.channel,totalChapters))
        val contentData = dataTextbook._2
        finlData = report++finlData
        contentD = contentData++contentD
      }
      (finlData,contentD)
    })

    val reportData = output.map(f=>f._1).flatten.toDF()
    val contents = output.map(f=>f._2).flatten.toDF()

    val textbookReport = reportData.join(contents, Seq("identifier"),"left_outer")

    val tenantInfo=getTenantInfo(RestUtil).toDF()
    textbookReport.show()
    tenantInfo.show()

    val finalDf = textbookReport.join(tenantInfo, Seq("channel"),"left_outer")
      .withColumn("reportName",lit("vdn-report"))

    val reportconfigMap = configMap("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))

    finalDf.show

    reportConfig.output.map { f =>
      CourseUtils.postDataToBlob(finalDf,f,configMap)
    }

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
