package org.sunbird.analytics.job.report

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{approx_count_distinct, broadcast, col, concat, count, countDistinct, lit, sum}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, JobContext, JobDriver, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.analytics.model
import org.sunbird.analytics.model.report.ETBMetricsModel.updateReportPath
import org.sunbird.analytics.model.report.VDNMetricsModel.{generateReport, getTenantInfo}
import org.sunbird.analytics.model.report.{ContentDetails, TenantInfo, TestContentdata, TextbookHierarchy, TextbookReportResult}
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}

case class ProgramData(program_id: String, name: String, slug: String, channel: String,
                       status: String, startdate: String, enddate: String)
case class NominationData(id: String, program_id: String, user_id: String,
                          status: String)
case class NominationDataV2(program_id: String, Initiated : String, Pending: String,
                            Rejected: String, Approved: String)

case class ProgramDataV2(program_id: String, name: String, slug: String,
                         channel: String, status: String, noOfUsers: Int)
case class ProgramVisitors(program_id:String, startdate:String,enddate:String,visitors:Int)

case class ContentES(result: ContentResultData, responseCode: String)
case class ContentES2(result: ContentResultData2, responseCode: String)
case class ContentResultData2(facets: List[ContentDataV4], count: Int)
case class ContentDataV4(values:List[ContributionData])
case class ContributionData(name:String,count:Int)
//case class ContentResultData(count: Int)
case class ContentResultData(content: List[ContentDataV3], count: Int)
case class ContentDataV3(acceptedContents: List[String],rejectedContents: List[String])
case class ContentValues(program_id: String,identifier: String,name: String, acceptedContents: Int,
                         rejectedContents: Int,mvcContributions: Int)

case class ContentDataV2(program_id: String,approvedContributions: Int,rejectedContents: Int,mvcContributions: Int, totalContributions: Int)

case class FunnelResult(program_id:String, reportDate: String, projectName: String, noOfUsers: String, initiatedNominations: String,
                        rejectedNominations: String, pendingNominations: String, acceptedNominations: String,
                        noOfContributors: String, noOfContributions: String, pendingContributions: String,
                        approvedContributions: String, slug: String)
case class VisitorResult(date: String, visitors: String, slug: String, reportName: String)
case class DruidTextbookData(visitors: Int)

object FunnelReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.report.FunnelReport"
  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps

  //  val db = "device_db"
  //  val url = "jdbc:postgresql://localhost:5432/"+ s"$db"
  //  val connProperties = getCon()

  val programTable = "program"
  val nominationTable = "nomination"

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
    getPostgresData(spark,configMap)

  }

  def getPostgresData(spark: SparkSession, config: Map[String,AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val encoder = Encoders.product[ProgramData]
    val programData = spark.read.jdbc(url, programTable, connProperties).as[ProgramData](encoder).rdd
      .map(f => (f.program_id,f))


    val encoders = Encoders.product[NominationDataV2]
    val nominationData = spark.read.jdbc(url, nominationTable, connProperties)

//    val contributors = nominationData.groupBy("program_id")
//      .agg(countDistinct("createdby").alias("contributors"))

    val nominations = nominationData.groupBy("program_id")
      .pivot(col("status"), Seq("Initiated","Pending","Rejected","Approved"))
      .agg(count("program_id"))
      .na.fill(0)

//    val rdd=contributors.join(nominations, Seq("program_id"),"inner")
    val rdd=nominations
      .as[NominationDataV2](encoders).rdd
      .map(f => (f.program_id,f))

    val reportDate = DateTimeFormat.forPattern("dd-MM-yyyy").print(DateTime.now())

    val data = programData.join(rdd)
    var druidData = List[ProgramVisitors]()
    val druidQuery = JSONUtils.serialize(config("druidConfig"))
    val report=data
      .filter(f=> null != f._2._1.status && f._2._1.status.equalsIgnoreCase("Live"))
      .map(f => {
        val datav2 = getESData(f._2._1.program_id)
        druidData = ProgramVisitors(f._2._1.program_id,f._2._1.startdate,f._2._1.enddate,0) :: druidData
        FunnelResult(f._2._1.program_id,reportDate,f._2._1.name,"0",f._2._2.Initiated,f._2._2.Rejected,
          f._2._2.Pending,f._2._2.Approved,datav2._1.toString,datav2._2.toString,datav2._3.toString,
          datav2._4.toString,f._2._1.slug)
      }).toDF()

//    val tenantInfo = getTenantInfo(RestUtil).map(f=>(f.id,f))
//    val funnelResult = FunnelResult("","","","","","","","","","","","","Unknown")
//
//    val finalDf=report.join(tenantInfo).map(f=>{
//      FunnelResult(f._2._1.program_id,f._2._1.reportDate,f._2._1.projectName,
//        f._2._1.noOfUsers,f._2._1.initiatedNominations,f._2._1.rejectedNominations,
//        f._2._1.pendingNominations,f._2._1.acceptedNominations,f._2._1.noOfContributors,
//        f._2._1.noOfContributions,f._2._1.pendingContributions,f._2._1.approvedContributions,
//        f._2._2.slug)
//    }).toDF()
//
//    val testd = report.leftOuterJoin(tenantInfo).map(f=>{
//      FunnelResult(f._2._1.program_id,f._2._1.reportDate,f._2._1.projectName,
//        f._2._1.noOfUsers,f._2._1.initiatedNominations,f._2._1.rejectedNominations,
//        f._2._1.pendingNominations,f._2._1.acceptedNominations,f._2._1.noOfContributors,
//        f._2._1.noOfContributions,f._2._1.pendingContributions,f._2._1.approvedContributions,
//        f._2._2.getOrElse(TenantInfo("","Unknown")).slug)
//    })
//
//    JobLogger.log(s"FunnelReport: Tenant info ${finalDf.count()}, ${report.count()}:${tenantInfo.count()}:::${testd.count()}", None, INFO)

//      .toDF().na.fill("Unknown", Seq("slug"))
//      .drop("program_id")
    val visitorData = druidData.map(f => {
      val query = getDruidQuery(druidQuery,f.program_id,s"${f.startdate.split(" ")(0)}T00:00:00+00:00/${f.enddate.split(" ")(0)}T00:00:00+00:00")
              val data = DruidDataFetcher.getDruidData(query).collect().map(f => JSONUtils.deserialize[DruidTextbookData](f))
              val noOfVisitors = if(data.nonEmpty) data.head.visitors else 0
      ProgramVisitors(f.program_id,f.startdate,f.enddate,noOfVisitors)
    }).toDF().na.fill(0)

    val funnelReport=report.join(visitorData,Seq("program_id"),"left")
      .drop("startdate","enddate","program_id","noOfUsers")

    val reportconfigMap = config("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig")
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))
    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val fieldsList = funnelReport.columns
    val filteredDf = funnelReport.select(fieldsList.head, fieldsList.tail: _*)
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
      .withColumn("reportName",lit("FunnelReport"))

//    JobLogger.log(s"FunnelReport: Saving dataframe to blob${funnelReport.count()}, ${report.count()}:${visitorData.count()}", None, INFO)

    val storageConfig = getStorageConfig("reports", "")
    renamedDf.saveToBlobStore(storageConfig, "csv", "",
      Option(Map("header" -> "true")), Option(List("slug","reportName")))

    //    val druidVisitorQuery = druidQuery
    //    val druidVisitor = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](druidVisitorQuery))

    //    val visitorData2 = druidVisitor.map(f=>JSONUtils.deserialize[DruidTextbookData](f)).map(f=>(f.program_id,f))
    //val visitorReport = visitorData2.join(programData).map(f=>VisitorResult(reportDate,f._2._1.visitors.toString,f._2._2.slug,"VisitorReport"))
    //  .toDF()
    //
    //    visitorReport.show
    //    report.saveToBlobStore(storageConfig,"csv", "",
    //      Option(Map("header" -> "true")), Option(List("slug","reportName")))
    //      .map(f=> FunnelResult(reportDate,f._2._1.name,"0",f._2._2.Initiated,f._2._2.Rejected,
    //      f._2._2.Pending,f._2._2.Approved,f._2._2.contributors,"0","0","0",f._2._1.slug,"FunnelReport") )
    //      .toDF().na.fill("Unknown", Seq("slug")).show(false)


    //    val filteredData = data.filter(f=> null != f._2._1.status && f._2._1.status.equalsIgnoreCase("Live"))
    //      .map(f=>f._2).toDF().show(false)

    //    filteredData.map(f=>f._2._2.status.equals())
    //    data.map(f=>f._2._2).groupBy(f => f.status).toDF().show

    //    programData.map(f=>f._2).toDF().show
    //    nominationData.map(f=>f._2).toDF().show
  }

  def getDruidQuery(query: String, programId: String, interval: String): DruidQueryModel = {
    val mapQuery = JSONUtils.deserialize[Map[String,AnyRef]](query)
    val filters = JSONUtils.deserialize[List[Map[String, AnyRef]]](JSONUtils.serialize(mapQuery("filters")))
    val updatedFilters = filters.map(f => {
      f map {
        case ("value","program_id") => "value" -> programId
        case x => x
      }
    })
    val finalMap = mapQuery.updated("filters",updatedFilters) map {
      case ("intervals","startdate/enddate") => "intervals" -> interval
      case x => x
    }
    JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(finalMap))
  }

  def getESData(programId: String): (Int,Int,Int,Int) = {
    val url = AppConf.getConfig("dock.service.search.url")

    val contributionRequest = s"""{
                                 |    "request": {
                                 |        "filters": {
                                 |            "objectType": "content",
                                 |            "status": ["Live"],
                                 |            "programId": "$programId",
                                 |            "mimeType": {"!=": "application/vnd.ekstep.content-collection"},
                                 |            "contentType": {"!=": "Asset"}
                                 |        },
                                 |        "not_exists": [
                                 |            "sampleContent"
                                 |        ],
                                 |        "facets":["createdBy"],
                                 |        "limit":0
                                 |    }
                                 |}""".stripMargin
    val contributionResponse = RestUtil.post[ContentES2](url,contributionRequest)
    val contributionResponses =if(null != contributionResponse && contributionResponse.responseCode.equalsIgnoreCase("OK") && contributionResponse.result.count>0) {
      contributionResponse.result.facets
    } else List()
    val totalContributors = contributionResponses.filter(p => null!=p.values).flatMap(f=>f.values).length
    val totalContributions=contributionResponses.filter(p => null!=p.values).flatMap(f=> f.values).map(f=>f.count).sum

//    val totalContributions = if(null != contributionResponse && contributionResponse.responseCode.equalsIgnoreCase("OK")) contributionResponse.result.count else 0

    val tbRequest = s"""{
                       |	"request": {
                       |       "filters": {
                       |         "programId": "$programId",
                       |         "objectType": "content",
                       |         "status": ["Draft","Live","Review"],
                       |         "contentType": "Textbook",
                       |         "mimeType": "application/vnd.ekstep.content-collection"
                       |       },
                       |       "fields": ["acceptedContents", "rejectedContents"],
                       |       "limit": 10000
                       |     }
                       |}""".stripMargin
    val response = RestUtil.post[ContentES](url,tbRequest)

    val contentData = if(null != response && response.responseCode.equalsIgnoreCase("OK") && response.result.count>0) {
      response.result.content
    } else List()
    val acceptedContents = contentData.filter(p => null!=p.acceptedContents).flatMap(f=>f.acceptedContents).length
    val rejectedContents = contentData.filter(p => null!=p.rejectedContents).flatMap(f=>f.rejectedContents).length
    val contents = acceptedContents+rejectedContents

    (totalContributors,totalContributions,totalContributions-contents,acceptedContents)

  }

  def getCon (): Properties = {
    //    val user = AppConf.getConfig("postgres.user")
    //    val pass = AppConf.getConfig("postgres.password")

    val connProperties = new Properties()
    connProperties.setProperty("driver", "org.postgresql.Driver")

    connProperties
  }

}

