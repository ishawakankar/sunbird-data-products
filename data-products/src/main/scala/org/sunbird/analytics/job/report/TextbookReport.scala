package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, concat, count, lit, split}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Level}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.job.report.CourseMetricsJobV2.getStorageConfig
import org.sunbird.analytics.model.report.VDNMetricsModel.getTenantInfo
import org.sunbird.analytics.model.report.{ContentHierarchy, TenantInfo, TestContentdata, TextbookData, TextbookHierarchy, TextbookReportResult}
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}
import org.sunbird.cloud.storage.conf.AppConf

case class TextbookReportClss(l1identifier:String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String)
case class FinalReportV2(identifier: String,l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, slug:String,reportName:String)

object TextbookReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.TextbookReport"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")

  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.log("Started execution - Textbook Report Job",None, Level.INFO)

    implicit val sparkContext: SparkContext = getReportingSparkContext(JSONUtils.deserialize[JobConfig](config))
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sparkContext.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val conf = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val tbs = getTextbooks(conf)


    computeReportV2(tbs,spark, config)


    JobLogger.log("Job execution completed successfully - Textbook Report Job",None, Level.INFO)
    frameworkContext.closeContext()
  }

  def computeReportV2(tbs: List[TextbookData], spark: SparkSession, config: String)(implicit sc: SparkContext,fc: FrameworkContext): Unit = {
    val encoders = Encoders.product[ContentHierarchy]

    var finlData = List[TextbookReportResult]()
    var contentD = List[TestContentdata]()
    JobLogger.log(s"Textbook map --",None, Level.INFO)

    val result = tbs.map(tb => {
      val tbHierarchy = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
        .where(col("identifier") === tb.identifier)
      val count = tbHierarchy.count()

      JobLogger.log(s"Getting tbHierarchy ${count}",None, Level.INFO)

      if(count > 0) {
        val tbRdd = tbHierarchy.as[ContentHierarchy](encoders).first()
        val hier = tbRdd.hierarchy
        val convertedHier = JSONUtils.deserialize[TextbookHierarchy](hier)
        val metricsTb = generateTbReport(List(convertedHier),List(), List(),convertedHier,List(),List("","0"))

        val tbRept = metricsTb._1
        val contentData = metricsTb._2
        val totalChapters = metricsTb._3

        val report = tbRept.map(f => TextbookReportResult(tb.identifier,f.l1identifier,tb.board,tb.medium,tb.gradeLevel,tb.subject,f.name,f.chapters,tb.channel,totalChapters))

        finlData = report.reverse ++ finlData
        contentD = contentData ++ contentD
        //        JobLogger.log(s"Parsing lengths ${tbRept.length},${contentData.length},${report.length},${finlData.length},${contentD.length}",None, Level.INFO)
      }

      (finlData,contentD)
    })

    //
    //    val result1 = result.flatMap(f=>f._1)
    //    val result2 = result.flatMap(f=>f._2)

    JobLogger.log("Parsed hierarchy for all textbooks",None, Level.INFO)
    //    JobLogger.log(s"Length for total parsed textbooks ${result.length}",None, Level.INFO)

    //    JobLogger.log(s"Length for parsed textbook info ${finlData.length} : ${contentD.length}",None, Level.INFO)
    //    JobLogger.log(s"After flattening both lists ${result1.length} : ${result2.length}",None, Level.INFO)


//    val finlDataRdd = sc.parallelize(finlData).map(f=>((f.identifier+","+f.l1identifier),f))
//    val contentDRdd = sc.parallelize(contentD).map(f=>((f.identifier+","+f.l1identifier),f))

//    val tbReport = TextbookReportResult("","","","","","","","","","")
//    val textbookReport = finlDataRdd.fullOuterJoin(contentDRdd).map(f=> (f._2._1.getOrElse(tbReport).channel,TbResult(f._2._1.getOrElse(tbReport).identifier,f._2._1.getOrElse(tbReport).l1identifier,
//      f._2._1.getOrElse(tbReport).board,f._2._1.getOrElse(tbReport).medium,
//      f._2._1.getOrElse(tbReport).grade,f._2._1.getOrElse(tbReport).subject,
//      f._2._1.getOrElse(tbReport).name,f._2._1.getOrElse(tbReport).chapters,
//      f._2._1.getOrElse(tbReport).channel,f._2._1.getOrElse(tbReport).totalChapters,
//      f._2._2.getOrElse(TestContentdata("","","")).contentType)))
//    JobLogger.log(s"VDNMetricsJob: joined reportRdd to contentRdd", None, INFO)

    val newDatRdd = sc.parallelize(finlData).map(f=>(f.channel,f))


    val tenantInfo = getTenantInfo(RestUtil).map(f=>(f.id,f))
    JobLogger.log(s"VDNMetricsJob: getting tenant info to map and join", None, INFO)

//    val testd = TbResult("","","","","","","","","","","")
//    val testd = FinalReportV2("","","","","","","","","","","","")
val testd = TextbookReportResult("","","","","","","","","","")

    val reportds = newDatRdd.fullOuterJoin(tenantInfo).map(f=> FinalReportV2(f._2._1.getOrElse(testd).identifier,f._2._1.getOrElse(testd).l1identifier,
      f._2._1.getOrElse(testd).board,f._2._1.getOrElse(testd).medium,f._2._1.getOrElse(testd).grade,
      f._2._1.getOrElse(testd).subject,f._2._1.getOrElse(testd).name,f._2._1.getOrElse(testd).chapters,
      f._2._1.getOrElse(testd).channel,f._2._1.getOrElse(testd).totalChapters,
      f._2._2.getOrElse(TenantInfo("","Unknown")).slug,"vdn-report"))
    JobLogger.log(s"VDNMetricsJob: joined tenant info to textbook report", None, INFO)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val report = reportds.toDF()
    val conRep = contentD.toDF()

    val contDfchap= conRep.groupBy("identifier","l1identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("l1identifier"))

    val contDfTb= conRep.groupBy("identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("identifier"))

    val withCon = report.join(contDfTb, Seq("identifier"),"inner")
      .drop("identifier","channel","id","chapters","l1identifier")
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name)

    val withConChap = report.join(contDfchap, Seq("identifier","l1identifier"),"inner")
      .drop("identifier","l1identifier","channel","id","totalChapters")
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name,'chapters)

//    //chapter level report
//    val chapterReport = report.groupBy(report.drop("contentType").columns.map(col): _*)
//      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("l1identifier"))
//      .drop("identifier","l1identifier","channel","id","totalChapters")
//      .na.fill(0)
    JobLogger.log(s"VDNMetricsJob: extracted chapter level", None, INFO)
//
//    //textbook level report
//    val textbookRepo = report.groupBy(report.drop("l1identifier","chapters","contentType").columns.map(col): _*)
//      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("identifier"))
//      .drop("identifier","channel","id")
//      .na.fill(0)
    JobLogger.log(s"VDNMetricsJob: extracted textbook level", None, INFO)

    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val reportconfigMap = configMap("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig")
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))


    val storageConfig = getStorageConfig("reports", "ChapterLevel")
    JobLogger.log(s"VDNMetricsJob: saving to blob $storageConfig, ${withConChap.count()}", None, INFO)

    withConChap.saveToBlobStore(storageConfig, "csv", "ChapterLevel",
      Option(Map("header" -> "true")), None)

//    reportConfig.output.map { f =>
//      val reportConf = reportconfigMap.asInstanceOf[Map[String, AnyRef]]
////      val mergeConf = reportConf.getOrElse("mergeConfig", Map()).asInstanceOf[Map[String,AnyRef]]
//
//      val storageConfig = getStorageConfig("reports", "ChapterLevel")
////      var reportMap = updateTbReportName(mergeConf, reportConf, "ChapterLevel.csv")
////      val tnp=configMap("modelParams").asInstanceOf[Map[String, AnyRef]].updated("reportConfig",reportMap)
////      val dims =  tnp.getOrElse("folderPrefix", List()).asInstanceOf[List[String]]
////      CourseUtils.postDataToBlob(withConChap,f,configMap("modelParams").asInstanceOf[Map[String, AnyRef]].updated("reportConfig",reportMap))
//
//      withConChap.saveToBlobStore(storageConfig, "csv", "druid", Option(Map("header" -> "true")), Option(Seq("slug", "reportName")))
////      withConChap.saveToBlobStore(storageConfig, "json", "", Option(Map("header" -> "true")), None)
//
////      reportMap = updateTbReportName(mergeConf, reportConf, "TextbookLevel.csv")
////      CourseUtils.postDataToBlob(withCon,f,configMap("modelParams").asInstanceOf[Map[String, AnyRef]].updated("reportConfig",reportMap))
////      withCon.saveToBlobStore(storageConfig, "csv", "", Option(Map("header" -> "true")), None)
////      withCon.saveToBlobStore(storageConfig, "json", "", Option(Map("header" -> "true")), None)
//    }
  }

  def updateTbReportName(mergeConf: Map[String,AnyRef], reportConfig: Map[String,AnyRef], reportPath: String): Map[String,AnyRef] = {
    val mergeMap = mergeConf map {
      case ("reportPath","vdn-report.csv") => "reportPath" -> reportPath
      case x => x
    }
    if(mergeMap.nonEmpty) reportConfig.updated("mergeConfig",mergeMap) else reportConfig
  }

  def generateTbReport(data: List[TextbookHierarchy], prevData: List[TextbookReportClss], newData: List[TextbookHierarchy],textbookInfo: TextbookHierarchy, contentInfo: List[TestContentdata], chapterInfo: List[String]): (List[TextbookReportClss],List[TestContentdata],String) = {
    var textbookReport = prevData
    var contentData = contentInfo
    var l1identifier = chapterInfo(0)
    var totalChapters = chapterInfo(1)
    var textbook = List[TextbookHierarchy]()

    data.map(units=> {
      val children = units.children
      if(units.depth==1) {
        textbook = units :: newData
        l1identifier = units.identifier
        val grade = TextBookUtils.getString(textbookInfo.gradeLevel)
        val report = TextbookReportClss(l1identifier,textbookInfo.board,TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name,textbookInfo.channel)
        totalChapters = (totalChapters.toInt+1).toString
        textbookReport = report :: textbookReport
      }

      if(units.depth!=0 && units.contentType.getOrElse("").nonEmpty) {
        contentData = TestContentdata(textbookInfo.identifier,l1identifier, units.contentType.get) :: contentData
      }

      if(children.isDefined) {
        val textbookReportData = generateTbReport(children.get, textbookReport, textbook,textbookInfo, contentData,List(l1identifier,totalChapters))
        textbookReport = textbookReportData._1
        contentData = textbookReportData._2
        totalChapters = textbookReportData._3
      }
    })

    (textbookReport,contentData,totalChapters)
  }

  def getTextbooks(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): List[TextbookData] = {
    val textbooks = TextBookUtils.getTextBooks(config, RestUtil)
    JobLogger.log(s"Fetched textbooks from druid ${textbooks.length}",None, Level.INFO)
    textbooks
  }

}
