package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql._
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseUtils, UserCache}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

trait ReportGeneratorV2 {

  def loadData(spark: SparkSession, settings: Map[String, String], url: String): DataFrame

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, fetchTable: (SparkSession, Map[String, String], String) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit
}

case class CourseData(userid: String, courseid: String, completionPercentage: Int)

object CourseMetricsJobV2 extends optional.Application with IJob with ReportGeneratorV2 with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.CourseMetricsJobV2"
  val sunbirdKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()

// $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def name(): String = "CourseMetricsJobV2"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))

    val conf = config.split(";")
    val batchIds = if(conf.length > 1) {
      conf(1).split(",").toList
    } else List()
    val jobConfig = JSONUtils.deserialize[JobConfig](conf(0))
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    execute(jobConfig, batchIds)
  }

  private def execute(config: JobConfig, batchList: List[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val time = CommonUtil.time({
      prepareReport(spark, storageConfig, loadData, config, batchList)
    })
    metrics.put("totalExecutionTime", time._1)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

// $COVERAGE-ON$ Enabling scoverage for all other functions
  def loadData(spark: SparkSession, settings: Map[String, String], url: String): DataFrame = {
    spark.read.format(url).options(settings).load()
  }

  def getActiveBatches(loadData: (SparkSession, Map[String, String], String) => DataFrame, batchList: List[String])
                      (implicit spark: SparkSession, fc: FrameworkContext): Array[Row] = {

    implicit  val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val courseBatchDF = if(batchList.nonEmpty) {
      loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra")
        .filter(batch => batchList.contains(batch.getString(1)))
        .select("courseid", "batchid", "enddate", "startdate")
    }
    else {
      loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra")
        .select("courseid", "batchid", "enddate", "startdate")
    }

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val comparisonDate = fmt.print(DateTime.now(DateTimeZone.UTC).minusDays(1))

    JobLogger.log("Filtering out inactive batches where date is >= " + comparisonDate, None, INFO)

    val activeBatches = courseBatchDF.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate)))
    val activeBatchList = activeBatches.select("courseid","batchid", "startdate", "enddate").collect
    JobLogger.log("Total number of active batches:" + activeBatchList.length, None, INFO)

    activeBatchList
  }

  def getUserCourseInfo(loadData: (SparkSession, Map[String, String], String) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val userAgg = loadData(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra")
      .select("user_id","activity_id","agg")

    val hierarchyData = loadData(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore), "org.apache.spark.sql.cassandra")
      .select("identifier","hierarchy")

    val userCourseDf = userAgg.join(hierarchyData, userAgg.col("activity_id") === hierarchyData.col("identifier"), "inner")

    userCourseDf.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String,AnyRef]](row.getString(4))
      val leafNodesCount = hierarchy.getOrElse("leafNodesCount",0).asInstanceOf[Int]
      val completedCount = row(2).asInstanceOf[Map[String,Int]]("completedCount")
      var completionPercentage = ((completedCount/leafNodesCount) * 100).abs
      completionPercentage = if (completionPercentage > 100) 100 else completionPercentage
      CourseData(row.getString(0), row.getString(1), completionPercentage)
    }).toDF()
  }

  def recordTime[R](block: => R, msg: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String], String) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession: SparkSession = spark
    val activeBatches = getActiveBatches(loadData, batchList)
    val userCourses = getUserCourseInfo(loadData)
    val userData = CommonUtil.time({
      recordTime(getUserData(spark, loadData), "Time taken to get generate the userData- ")
    })
    val activeBatchesCount = new AtomicInteger(activeBatches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))

    for (index <- activeBatches.indices) {
      val row = activeBatches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      if(courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
        val result = CommonUtil.time({
          val reportDF = recordTime(getReportDF(batch, userData._2, userCourses, loadData), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val totalRecords = reportDF.count()
          recordTime(saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords), s"Time taken to save report in blobstore for batch ${batch.batchid} - ")
          reportDF.unpersist(true)
        })
        JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      }
    }
    userData._2.unpersist(true)

  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String) => DataFrame): DataFrame = {
    loadData(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"), "org.apache.spark.sql.redis")
      .select(col(UserCache.userid),col(UserCache.firstname),col(UserCache.lastname),col(UserCache.schoolname),col(UserCache.district),col(UserCache.orgname),col(UserCache.schooludisecode),
        col(UserCache.maskedemail),col(UserCache.state),col(UserCache.externalid),col(UserCache.block),col(UserCache.maskedphone), col(UserCache.userchannel),
        concat_ws(" ", col("firstname"), col("lastname")).as("username"))
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, courseDf: DataFrame,loadData: (SparkSession, Map[String, String], String) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val userCourseDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra")
      .select(col("batchid"), col("userid"), col("courseid"), col("active"), col("certificates")
        , col("enrolleddate"), col("completedon"))
      /*
       * courseBatchDF has details about the course and batch details for which we have to prepare the report
       * courseBatchDF is the primary source for the report
       * userCourseDF has details about the user details enrolled for a particular course/batch
       */
      .where(col("batchid") === batch.batchid && lower(col("active")).equalTo("true"))
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("channel", lit(batch.courseChannel))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("certificate_status", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .select(
        col("batchid"),
        col("userid"),
        col("enddate"),
        col("startdate"),
        col("enrolleddate"),
        col("completedon"),
        col("active"),
        col("courseid"),
        col("generatedOn"),
        col("certificate_status"),
        col("channel")
      )

    val userCourseDenormDF = userCourseDF.join(courseDf, Seq("userid"), "inner")
      .select(userCourseDF.col("*"),
        courseDf.col("completionPercentage").as("course_completion"))

    // userCourseDenormDF lacks some of the user information that need to be part of the report here, it will add some more user details
    val reportDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .withColumn(UserCache.externalid, when(userCourseDenormDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.externalid)).otherwise(""))
      .withColumn(UserCache.schoolname, when(userCourseDenormDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schoolname)).otherwise(""))
      .withColumn(UserCache.block, when(userCourseDenormDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.block)).otherwise(""))
      .withColumn(UserCache.schooludisecode, when(userCourseDenormDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schooludisecode)).otherwise(""))
      .select(
        userCourseDenormDF.col("*"),
        col("channel"),
        col(UserCache.firstname),
        col(UserCache.lastname),
        col(UserCache.maskedemail),
        col(UserCache.maskedphone),
        col(UserCache.externalid),
        col(UserCache.orgname),
        col(UserCache.schoolname),
        col(UserCache.district),
        col(UserCache.schooludisecode),
        col(UserCache.block),
        col(UserCache.state)
      ).persist()
    reportDF
  }

  def saveReportToBlobStore(batch: CourseBatch, reportDF: DataFrame, storageConfig: StorageConfig, totalRecords:Long): Unit = {
    reportDF
      .select(
        col(UserCache.externalid).as("External ID"),
        col(UserCache.userid).as("User ID"),
        concat_ws(" ", col(UserCache.firstname), col(UserCache.lastname)).as("User Name"),
        col(UserCache.maskedemail).as("Email ID"),
        col(UserCache.maskedphone).as("Mobile Number"),
        col(UserCache.orgname).as("Organisation Name"),
        col(UserCache.state).as("State Name"),
        col(UserCache.district).as("District Name"),
        col(UserCache.schooludisecode).as("School UDISE Code"),
        col(UserCache.schoolname).as("School Name"),
        col(UserCache.block).as("Block Name"),
        col("enrolleddate").as("Enrolment Date"),
        concat(col("course_completion").cast("string"), lit("%"))
          .as("Course Progress"),
        col("completedon").as("Completion Date"),
        col("certificate_status").as("Certificate Status"))
      .saveToBlobStore(storageConfig, "csv", "course-progress-reports/" + "report-" + batch.batchid, Option(Map("header" -> "true")), None)
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords }} ", None, INFO)
  }

}