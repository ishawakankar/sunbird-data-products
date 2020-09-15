package org.sunbird.analytics.model.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, count, lit}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.framework.{AlgoInput, Empty, FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.util.{Constants, CourseUtils, TextBookUtils}
import org.sunbird.cloud.storage.conf.AppConf

case class TextbookResponse(result: TextbookResult, responseCode: String)
case class TextbookResult(count: Int, content: List[TextbookInfo])
case class TextbookInfo(identifier: String, name: String, channel: String)

case class TextbookHierarchy(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                             name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
                             depth: Int, createdOn: String, children: Option[List[TextbookHierarchy]], index: Int, parent: String)
case class ContentHierarchy(identifier: String, hierarchy: String) extends AlgoInput

case class TextbookReport(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String)
case class ContentData(contentType: String, count: Int)
case class TestContentdata(identifier: String, l1identifier: String, contentType: String)

case class TextbookReportResult(identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, totalChapters: String)

object VDNMetricsModel extends IBatchModelTemplate[Empty,ContentHierarchy,Empty,Empty] with Serializable {

  implicit val className: String = "org.sunbird.analytics.model.report.VDNMetricsModel"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")

  override def name: String = "VDNMetricsModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentHierarchy] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val reportFilters = config.getOrElse("reportFilters", Map()).asInstanceOf[Map[String, AnyRef]]

//    val conf = new SparkConf().setAppName("AnalyticsTestSuite").set("spark.default.parallelism", "2");
//    conf.set("spark.sql.shuffle.partitions", "2")
//    conf.setMaster("local[*]")
//    conf.set("spark.driver.memory", "1g")
//    conf.set("spark.memory.fraction", "0.3")
//    conf.set("spark.memory.storageFraction", "0.5")
//    conf.set("spark.cassandra.connection.host", "localhost")
//    conf.set("spark.cassandra.connection.port", "9042")
//    conf.set("es.nodes", "http://localhost")

    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val contents=spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()

//    val contents = if(reportFilters.nonEmpty) {
//      println("non empty")
//      val filteredTextbooks = getTextbooks(JSONUtils.serialize(reportFilters))
//      println(filteredTextbooks)
//      spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> "sunbird_courses")).load()
//        .filter(identifiers => filteredTextbooks.contains(identifiers.getString(0)))
//        .select("identifier","hierarchy")
//    } else spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> "sunbird_courses")).load()

    contents.show

    val encoder = Encoders.product[ContentHierarchy]
    contents.as[ContentHierarchy](encoder).rdd
  }

  override def algorithm(events: RDD[ContentHierarchy], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var finlData = List[TextbookReportResult]()
    var contentD = List[TestContentdata]()
    val output=events.collect().map(f => {
      val hierarchy = f.hierarchy
      val data = JSONUtils.deserialize[TextbookHierarchy](hierarchy)
      val dataTextbook = generateReport(List(data), List(), List(),data,List(),List("","0"))

      val textbookReport = dataTextbook._1
      val totalChapters = dataTextbook._3
      val report = textbookReport.map(f=>TextbookReportResult(f.identifier,f.board,f.medium,f.grade,f.subject,f.name,f.chapters,totalChapters))
      val contentData = dataTextbook._2
      finlData = report++finlData
      contentD = contentData++contentD
      (report,contentData)
    })
    output.foreach(f=>println(f))

    val reportData=finlData.toDF()
    val contents = contentD.toDF()

    val df = reportData.join(contents,Seq("identifier"),"left_outer")
      .withColumn("slug",lit("unknown"))
      .withColumn("reportName", lit("content-data"))


    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    reportConfig.output.map { f =>
      CourseUtils.postDataToBlob(df,f,config)
    }

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
          val report = TextbookReport(l1identifier,textbookInfo.board,TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name)
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

}
