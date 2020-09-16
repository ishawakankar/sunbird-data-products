package org.sunbird.analytics.model.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, count, lit}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.{CommonUtil, HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{AlgoInput, Empty, FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}
import org.sunbird.cloud.storage.conf.AppConf

case class TextbookResponse(result: TextbookResult, responseCode: String)
case class TextbookResult(count: Int, content: List[TextbookInfo])
case class TextbookInfo(identifier: String, name: String, channel: String)

case class TextbookHierarchy(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                             name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
                             depth: Int, createdOn: String, children: Option[List[TextbookHierarchy]], index: Int, parent: String)
case class ContentHierarchy(identifier: String, hierarchy: String)

case class TextbookReport(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String)
case class ContentData(contentType: String, count: Int)
case class TestContentdata(identifier: String, l1identifier: String, contentType: String)

case class TextbookReportResult(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String)
case class TextbookReportResults(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String,reportName:String,slug:String)
case class TBResult(tbReport: TextbookReportResult, contentD: TestContentdata)

object VDNMetricsModel extends IBatchModelTemplate[Empty,Empty,Empty,Empty] with Serializable {

  implicit val className: String = "org.sunbird.analytics.model.report.VDNMetricsModel"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")

  override def name: String = "VDNMetricsModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    sc.emptyRDD
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val contentDf=spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
    contentDf.persist(StorageLevel.MEMORY_ONLY)
    val encoder = Encoders.product[ContentHierarchy]
    val eventDf=contentDf.as[ContentHierarchy](encoder).rdd

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var finlData = List[TextbookReportResult]()
    var contentD = List[TestContentdata]()
    JobLogger.log(s"VDNMetricsJob: Processing dataframe", None, INFO)

    JobLogger.log(s"VDNMetricsJob: event size: ${events.count()}", None, INFO)
    val output=eventDf.map(f => {
      val hierarchy = f.hierarchy
      val data = JSONUtils.deserialize[TextbookHierarchy](hierarchy)
      if(data.contentType!=null && data.contentType.getOrElse("").equalsIgnoreCase("Textbook")) {
        val dataTextbook = generateReport(List(data), List(), List(),data,List(),List("","0"))
        val textbookReport = dataTextbook._1
        val totalChapters = dataTextbook._3
        val report = textbookReport.map(f=>TextbookReportResult(f.identifier,f.board,f.medium,f.grade,f.subject,f.name,f.chapters,f.channel,totalChapters))
        val contentData = dataTextbook._2
        finlData = report++finlData
        contentD = contentData++contentD
      }
      (finlData,contentD)

    })

    JobLogger.log(s"VDNMetricsJob: output size: ${output.count()}", None, INFO)

    val reportData = output.map(f=>f._1).flatMap(f=>f).map(f=>(f.identifier,f))
    val contents = output.map(f=>f._2).flatMap(f=>f).map(f=>(f.identifier,f))

    JobLogger.log(s"VDNMetricsJob: Flattening reports", None, INFO)

    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
    JobLogger.log(s"VDNMetricsJob: reportconfig: ${reportConfig.output}", None, INFO)


    JobLogger.log(s"VDNMetricsJob: saving report df: ${reportData.count()}", None, INFO)
    val tbResult = TextbookReportResult("","","","","","","","","")
    val df = reportData.fullOuterJoin(contents).map(f=>{
      (f._2._1.getOrElse(tbResult).channel,f._2._1.getOrElse(tbResult))
    })

    val tenantInfo=getTenantInfo(RestUtil).map(e => (e.id,e))
    val finalDf=df.fullOuterJoin(tenantInfo).map(f=>{
      val data= f._2._1.getOrElse(tbResult)
      TextbookReportResults(data.identifier,data.board,data.medium,data.grade,data.subject,data.name,data.chapters,data.channel,
      data.totalChapters,"vdn-report",f._2._2.getOrElse(TenantInfo("","")).slug)
      }).toDF()

    JobLogger.log(s"VDNMetricsJob: records stats before cloud upload: No of records: ${finalDf.count()}", None, INFO)

    reportConfig.output.map { f =>
      CourseUtils.postDataToBlob(finalDf,f,config)
    }

    contentDf.unpersist(true)

    sc.emptyRDD
  }

  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    println("in post process")
    sc.emptyRDD
  }

  def getTextbooks(query: String): List[String] = {
    val apiUrl = Constants.COMPOSITE_SEARCH_URL
    val response = RestUtil.post[TextbookResponse](apiUrl, query)
    if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
      response.result.content.map(f => f.identifier)
    } else List[String]()
  }

  def generateReport(data: List[TextbookHierarchy], prevData: List[TextbookReport], newData: List[TextbookHierarchy],textbookInfo: TextbookHierarchy, contentInfo: List[TestContentdata], chapterInfo: List[String]): (List[TextbookReport],List[TestContentdata],String) = {
    var textbookReport = prevData
    var contentData = contentInfo
    var l1identifier = chapterInfo(0)
    var totalChapters = chapterInfo(1)
    var textbook = List[TextbookHierarchy]()

      data.map(units=> {
        val children = units.children
        if(units.depth==1) {
          textbook = units :: newData
          val contentType = units.contentType.getOrElse("")
          l1identifier = units.identifier
          val grade = TextBookUtils.getString(textbookInfo.gradeLevel)
          val report = TextbookReport(l1identifier,textbookInfo.board,TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name,textbookInfo.channel)
          totalChapters = (totalChapters.toInt+1).toString
          textbookReport = report :: textbookReport
        }

        if(units.depth!=0 && units.contentType.getOrElse("").nonEmpty) {
          contentData = TestContentdata(textbookInfo.identifier,l1identifier, units.contentType.get) :: contentData
        }

        if(children.isDefined) {
          val textbookReportData = generateReport(children.get, textbookReport, textbook,textbookInfo, contentData,List(l1identifier,totalChapters))
          textbookReport = textbookReportData._1
          contentData = textbookReportData._2
          totalChapters = textbookReportData._3
        }
    })


    (textbookReport,contentData,totalChapters)
  }

  def getTenantInfo(restUtil: HTTPClient)(implicit sc: SparkContext):  RDD[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL

    val tenantRequest = s"""{
                           |    "params": { },
                           |    "request":{
                           |        "filters": {"isRootOrg":"true"},
                           |        "offset": 0,
                           |        "limit": 1000,
                           |        "fields": ["id", "channel", "slug", "orgName"]
                           |    }
                           |}""".stripMargin
    sc.parallelize(restUtil.post[TenantResponse](url, tenantRequest).result.response.content)
  }

}
