package org.sunbird.analytics.sourcing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, collect_list, collect_set, lit, when}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, Level, StorageConfig}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.sourcing.FunnelReport.{connProperties, programTable, url}
import org.sunbird.analytics.sourcing.SourcingMetrics.getTenantInfo

case class ContentDetails(identifier: String, name: String, board: String, medium: String, gradeLevel: String, subject: String, acceptedContents: String, rejectedContents: String, programId: String)
case class ContentDetailsResponse(identifier: String, collectionId: String, name: String, contentType: String, unitIdentifiers: String, createdBy: String, creator: String, mimeType: String)
case class FinalResultDF(programId: String, board: String, medium: String, gradeLevel: String, subject: String, contentId: String,
                         contentName: String, name: String, chapterName: String, contentType: String, mimeType: String, chapterId: String, contentStatus: String,
                         creator: String, identifier: String, createdBy: String)
case class ChapterInfo(name: String, unitIdentifiers: String, identifier: String)

object ContentDetailsReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.sourcing.ContentDetailsReport"
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
    implicit val sparkCon = sc.sparkContext
    import sc.implicits._
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val tenantId = configMap.getOrElse("tenantId","").asInstanceOf[String]
    val slug = configMap.getOrElse("slug","").asInstanceOf[String]
    if(tenantId.nonEmpty && slug.nonEmpty) {
      generateTenantReport(tenantId, slug, config)
    }
    else {
      val tenantInfo = getTenantInfo(RestUtil).collect().toList
      tenantInfo.map(f => {
        generateTenantReport(f.id,f.slug,config)
      })
    }

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

  def generateTenantReport(channel: String, slug: String, config: String)(implicit sc: SparkSession, fc: FrameworkContext): Unit = {
    implicit val sparkCon = sc.sparkContext
    import sc.implicits._

    JobLogger.log(s"Generating report for channel - $channel and slug - $slug", None, INFO)
    val query = s"""{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "programId","aliasName": "programId"}, {"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"}, {"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"},{"type": "isnotnull","dimension": "programId"}, {"type": "in","dimension": "status","values": ["Draft"]}, {"type": "equals","dimension": "channel","value": "$channel"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin

    val response = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](query),true)

    val textbooks = response.map(f=> JSONUtils.deserialize[ContentDetails](f)).toDF()
    JobLogger.log(s"textbook count - ${textbooks.count()}",None, Level.INFO)
    val contents = getContents(channel).toDF().withColumnRenamed("identifier","contentId")
      .withColumnRenamed("name","contentName")
    JobLogger.log(s"contents count - ${contents.count()}",None, Level.INFO)
    val df = contents.join(textbooks, contents.col("collectionId") === textbooks.col("identifier"), "inner")

    val newDf = df
      .groupBy("contentId","contentName","contentType","identifier",
        "name","board","medium","gradeLevel","subject","programId","createdBy",
        "creator","mimeType","unitIdentifiers")
      .agg(collect_list("acceptedContents").as("acceptedContents"),collect_list("rejectedContents").as("rejectedContents"))

    val resDf = newDf.rdd.map(f => {
      val contentStatus = if(f.getAs[Seq[String]](14).contains(f.getString(0))) "Approved" else if(f.getAs[Seq[String]](15).contains(f.getString(0))) "Rejected" else "Pending"
      FinalResultDF(f.getString(9), f.getString(5), f.getString(6), f.getString(7), f.getString(8),
        f.getString(0),f.getString(1),f.getString(4),"",f.getString(2),f.getString(12),
        f.getString(13),contentStatus,f.getString(11),f.getString(3),f.getString(10))
    }).toDF().withColumn("slug",lit(slug))

    val programData = sc.read.jdbc(url, programTable, connProperties)
      .select(col("program_id"), col("name").as("programName"))
      .persist(StorageLevel.MEMORY_ONLY)

    JobLogger.log(s"programData count - ${programData.count()}",None, Level.INFO)

    val finalDf = programData.join(resDf, programData.col("program_id") === resDf.col("programId"), "inner")
      .drop("program_id").persist(StorageLevel.MEMORY_ONLY)
    finalDf.show()
    JobLogger.log(s"finalDf count - ${finalDf.count()}",None, Level.INFO)

    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val storageConfig = getStorageConfig(jobConfig, "")
    val labels = Map("programName"->"Project Name","programId"->"Project ID","board"->"Board",
      "medium"->"Medium","gradeLevel"->"Grade","subject"->"Subject","contentId"->"Content ID",
      "contentName"->"Content Name","name"->"Textbook Name","contentType"->"Content Type",
      "mimeType"->"MimeType","chapterId"->"Chapter ID","contentStatus"->"Content Status",
      "creator"->"Creator Name","identifier"->"Textbook ID","createdBy"->"CreatedBy ID","chapterName"->"Chapter Name")

    finalDf.select(finalDf.columns.map(c => finalDf.col(c).as(labels.getOrElse(c, c))): _*)
      .withColumn("reportName",lit("ContentDetailsReport"))
      .saveToBlobStore(storageConfig, "csv", "sourcing",
        Option(Map("header" -> "true")), Option(List("slug","reportName")))
    JobLogger.log(s"Completed execution",None, Level.INFO)

    finalDf.unpersist(true)
    programData.unpersist(true)
  }

  def getContents(channel: String)(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentDetailsResponse] = {
    val query = s"""{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "contentType","aliasName": "contentType"}, {"fieldName": "unitIdentifiers","aliasName": "unitIdentifiers"}, {"fieldName": "collectionId","aliasName": "collectionId"}, {"fieldName": "createdBy","aliasName": "createdBy"}, {"fieldName": "creator","aliasName": "creator"}, {"fieldName": "mimeType","aliasName": "mimeType"}],"filters": [{"type": "notequals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live"]}, {"type": "equals","dimension": "channel","value": "$channel"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin
    val response = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](query), true)

    response.map(f => JSONUtils.deserialize[ContentDetailsResponse](f))
  }

  def getChapterInfo(channel: String)(implicit sc: SparkContext, fc: FrameworkContext): RDD[ChapterInfo] = {
    val query = s"""{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "unitIdentifiers","aliasName": "unitIdentifiers"},{"fieldName": "name","aliasName": "name"},{"fieldName": "identifier","aliasName": "identifier"}],"filters": [{"type": "isnotnull","dimension": "unitIdentifiers"}, {"type": "in","dimension": "status","values": ["Live"]}, {"type": "equals","dimension": "channel","value": "$channel"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin
    val response = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](query), true)
    response.map(f => JSONUtils.deserialize[ChapterInfo](f))
  }

}
