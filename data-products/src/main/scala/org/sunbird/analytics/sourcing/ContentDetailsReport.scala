package org.sunbird.analytics.sourcing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, collect_list, collect_set, lit, when}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, Level, StorageConfig}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.CourseMetricsJobV2.getStorageConfig
import org.sunbird.analytics.sourcing.SourcingMetrics.getTenantInfo
import org.sunbird.analytics.util.CourseUtils

case class ContentDetails(identifier: String, name: String, board: String, medium: String, gradeLevel: String, subject: String, acceptedContents: String, rejectedContents: String)
case class ContentDetailsResponse(identifier: String, collectionId: String, name: String, contentType: String, unitIdentifiers: String)
case class FinalResultDF(identifier: String, name: String, board: String, medium: String, gradeLevel: String, subject: String, contentId: String, contentName: String, contentType: String, contentStatus: String, chapterId: String)

object ContentDetailsReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.ContentDetailsReport"
  val jobName: String = "Content Details Job"

  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.log(s"Started execution - $jobName",None, Level.INFO)
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark = openSparkSession(jobConfig)

    try {
      val res = CommonUtil.time(execute(config))
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def execute(config: String)(implicit sc: SparkSession, fc: FrameworkContext): Unit = {

//    val tenantInfo = getTenantInfo(RestUtil).collect().toList
    generateTenantReport("f.id","f.slug")

//    val query = """{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"},{"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"},{"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live","Draft"]},{"type": "equals","dimension": "channel","value": "01309282781705830427"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin
//

  }

  def getQuery(query: String, channel: String): DruidQueryModel = {
    val mapQuery = JSONUtils.deserialize[Map[String,AnyRef]](query)
    val filters = JSONUtils.deserialize[List[Map[String, AnyRef]]](JSONUtils.serialize(mapQuery("filters")))
    val updatedFilters = filters.map(f => {
      f map {
        case ("value","channelId") => "value" -> channel
        case x => x
      }
    })
    val finalMap = mapQuery.updated("filters",updatedFilters)
    JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(finalMap))
  }

  def generateTenantReport(channel: String, slug: String)(implicit sc: SparkSession, fc: FrameworkContext): Unit = {
    implicit val sparkCon = sc.sparkContext
    import sc.implicits._

    JobLogger.log(s"Generating report for channel - $channel and slug - $slug", None, INFO)
    val query = """{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"}, {"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live", "Draft"]}, {"type": "equals","dimension": "channel","value": "01309282781705830427"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin

    val response = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](query),true)

        val textbooks = response.map(f=> JSONUtils.deserialize[ContentDetails](f)).toDF()
        val contents = getContents().toDF().withColumnRenamed("identifier","contentId")
          .withColumnRenamed("name","contentName")

        val df = contents.join(textbooks, contents.col("collectionId") === textbooks.col("identifier"), "inner")

        val newDf=df
          .groupBy("contentId","contentName","contentType","identifier",
          "name","board","medium","gradeLevel","subject","unitIdentifiers")
          .agg(collect_list("acceptedContents").as("acceptedContents"),collect_list("rejectedContents").as("rejectedContents"))

        val calcDf = newDf.rdd.map(f => {
          val contentStatus = if(f.getAs[Seq[String]](10).contains(f.getString(0))) "Approved" else if(f.getAs[Seq[String]](11).contains(f.getString(0))) "Rejected" else "Pending"
          FinalResultDF(f.getString(3),f.getString(4),f.getString(5),f.getString(6),f.getString(7),f.getString(8),f.getString(0)
          ,f.getString(1),f.getString(2),contentStatus,f.getString(9))
        }).toDF().withColumn("slug",lit("testSlug"))
          .withColumn("reportName",lit("ContentDetailsReport"))

    implicit val jobConfig = JSONUtils.deserialize[JobConfig](AppConf.getConfig("content.details.config"))
val storageConfig = getStorageConfig(jobConfig, "")
    calcDf.saveToBlobStore(storageConfig, "csv", "content-details",
      Option(Map("header" -> "true")), Option(List("slug","reportName")))

//        calcDf.show
  }

  def getContents()(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentDetailsResponse] = {
    val query = """{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"},  {"fieldName": "contentType","aliasName": "contentType"},  {"fieldName": "unitIdentifiers","aliasName": "unitIdentifiers"}, {"fieldName": "collectionId","aliasName": "collectionId"}],"filters": [{"type": "notequals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live", "Draft"]}, {"type": "equals","dimension": "channel","value": "01309282781705830427"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin
    val response = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](query), true)

    response.map(f => JSONUtils.deserialize[ContentDetailsResponse](f))
  }

}
