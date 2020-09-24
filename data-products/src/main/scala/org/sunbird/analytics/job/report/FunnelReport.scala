package org.sunbird.analytics.job.report

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{approx_count_distinct, broadcast, col, concat, count, countDistinct, lit, sum}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobContext, JobDriver, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.analytics.model
import org.sunbird.analytics.model.report.ETBMetricsModel.updateReportPath
import org.sunbird.analytics.model.report.VDNMetricsModel.{generateReport, getTenantInfo}
import org.sunbird.analytics.model.report.{ContentDetails, TenantInfo, TestContentdata, TextbookHierarchy, TextbookReportResult}
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}

case class ProgramData(program_id: String, name: String, slug: String,
                        channel: String, status: String)
case class NominationData(id: String, program_id: String, user_id: String,
                          status: String)
case class NominationDataV2(program_id: String, Initiated : String, Pending: String,
                            Rejected: String, Approved: String, contributors: String)

case class ContentES(result: ContentResultData, responseCode: String)
//case class ContentResultData(count: Int)
case class ContentResultData(content: List[ContentDataV3], count: Int)
case class ContentDataV3(identifier: String,name: String, acceptedContents: List[String],
                         rejectedContents: List[String],mvcContributions: List[String])
case class ContentValues(program_id: String,identifier: String,name: String, acceptedContents: Int,
                         rejectedContents: Int,mvcContributions: Int)

case class ContentDataV2(program_id: String,approvedContributions: Int,rejectedContents: Int,mvcContributions: Int, totalContributions: Int)

case class FunnelResult(reportDate: String, projectName: String, noOfUsers: String, initiatedNominations: String,
                       rejectedNominations: String, pendingNominations: String, acceptedNominations: String,
                       noOfContributors: String, noOfContributions: String, pendingContributions: String,
                       approvedContributions: String, slug: String, reportName: String)

object FunnelReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.report.FunnelReport"
  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps

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
    getPostgresData(spark)

  }

  def getPostgresData(spark: SparkSession)(implicit sc: SparkContext): Unit = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val encoder = Encoders.product[ProgramData]
    val programData = spark.read.jdbc(url, programTable, connProperties).as[ProgramData](encoder).rdd
      .map(f => (f.program_id,f))

    val encoders = Encoders.product[NominationDataV2]
    val nominationData = spark.read.jdbc(url, nominationTable, connProperties)

    val contributors = nominationData.groupBy("program_id")
      .agg(countDistinct("createdby").alias("contributors"))

    val nominations = nominationData.groupBy("program_id")
      .pivot(col("status"), Seq("Initiated","Pending","Rejected","Approved"))
      .agg(count("program_id"))
      .na.fill(0)

    val rdd=contributors.join(nominations, Seq("program_id"),"inner")
      .as[NominationDataV2](encoders).rdd
      .map(f => (f.program_id,f))

    val reportDate = DateTimeFormat.forPattern("dd-MM-yyyy").print(DateTime.now())

    val contentEncoder = Encoders.product[ContentDataV2]
    val programId=""

    val data = programData.join(rdd)
    val report=data.filter(f=> null != f._2._1.status && f._2._1.status.equalsIgnoreCase("Live"))
      .map(f=> {
//        val datav2 = getESData(f._2._1.program_id,contentEncoder)
//        val contributionsInfo = if(datav2.nonEmpty) datav2.head else Row("","0","0","0","0")
val contributionsInfo = Row("","0","0","0","0")
        FunnelResult(reportDate,f._2._1.name,"0",f._2._2.Initiated,f._2._2.Rejected,
          f._2._2.Pending,f._2._2.Approved,f._2._2.contributors,
          contributionsInfo.getString(4),
            contributionsInfo.getString(2),
        contributionsInfo.getString(1),f._2._1.slug,"FunnelReport")
      })
      .toDF().na.fill("Unknown", Seq("slug"))

    val storageConfig = getStorageConfig("reports", "")

    report.saveToBlobStore(storageConfig,"csv", "",
      Option(Map("header" -> "true")), Option(List("slug","reportName")))
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

  def getESData(programId: String, contentEncoder: Encoder[ContentDataV2])(implicit sc: SparkContext): List[Row] = {
List()
  }

  def getContributions(data: List[String]): Int = {
    if(null != data) {
      data.length
    } else 0
  }

  def getCon (): Properties = {
    val user = AppConf.getConfig("postgres.user")
    val pass = AppConf.getConfig("postgres.password")

    val connProperties = new Properties()
    connProperties.setProperty("driver", "org.postgresql.Driver")
    connProperties.setProperty("user", user)
    connProperties.setProperty("password", pass)
    connProperties
  }

}

