package org.sunbird.analytics.job.report

import java.io.File
import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{CourseUtils, UserData}

import scala.collection.mutable
import scala.concurrent.Future

case class UserAgg(activity_type:String,activity_id:String, user_id:String,context_id:String, agg: Map[String,Int],agg_last_updated:String)
case class ContentHierarchy(identifier: String, hierarchy: String)

class TestCourseMetricsJobV2 extends BaseReportSpec with MockFactory with BaseReportsJob {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var userEnrolmentDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var systemSettingDF: DataFrame = _
  var userAggDF: DataFrame = _
  var contentHierarchyDF: DataFrame = _
  var reporterMock: ReportGeneratorV2 = mock[ReportGeneratorV2]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdHierarchyStore = "dev_hierarchy_store"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {

    super.beforeAll()
    spark = getSparkSession()

    courseBatchDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/course_batch_data.csv").cache()

    externalIdentityDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/user_external_data.csv").cache()

    userCoursesDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/user_courses_data.csv").cache()

    userDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json").cache()

    systemSettingDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/system_settings.csv").cache()

    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    userAggDF = List(UserAgg("Course","do_1130314965721088001129","c7ef3848-bbdb-4219-8344-817d5b8103fa","cb:0130561083009187841",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course","do_13456760076615812","f3dd58a4-a56f-4c1d-95cf-3231927a28e9","cb:0130561083009187841",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course","do_1125105431453532161282","be28a4-a56f-4c1d-95cf-3231927a28e9","cb:0130561083009187841",Map("completedCount"->5),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}")).toDF()

    contentHierarchyDF = List(ContentHierarchy("do_1130314965721088001129","""{"mimeType": "application/vnd.ekstep.content-collection","children": [{"children": [{"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282","visibility": "Parent","name": "Untitled sub Course Unit 1.2"}],"mimeType": "collection","contentType": "Course","visibility": "Default","identifier": "do_1125105431453532161282","leafNodesCount": 3}, {"contentType": "Course","identifier": "do_1125105431453532161282","name": "Untitled Course Unit 2"}],"contentType": "Course","identifier": "do_1130314965721088001129","visibility": "Default","leafNodesCount": 9}"""),
      ContentHierarchy("do_13456760076615812","""{"mimeType": "application/vnd.ekstep.content-collection","children": [{"children": [{"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282","visibility": "Parent","name": "Untitled sub Course Unit 1.2"}],"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282"}, {"contentType": "CourseUnit","identifier": "do_1125105431453532161282","name": "Untitled Course Unit 2"}],"contentType": "Course","identifier": "do_13456760076615812","visibility": "Default","leafNodesCount": 4}""")).toDF()
  }

  "TestUpdateCourseMetricsV2" should "generate reports for batches and validate all scenarios" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    val schema = Encoders.product[UserData].schema
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(userAggDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(contentHierarchyDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)

    CourseMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra", new StructType())


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(systemSettingDF)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJobV2","modelParams":{"batchFilters":["TPD"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |  "identifier": "do_1130264512015646721166",
        |  "channel": "01274266675936460840172"
        |}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc));
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig: DruidConfig = DruidConfig.DefaultConfig

    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, config, List())

    implicit val sc = spark
    val courseBatchInfo = CourseMetricsJobV2.getUserCourseInfo(reporterMock.loadData)

    val batchInfo = List(CourseBatch("01303150537737011211","2020-05-29","2030-06-30","b00bc992ef25f1a9a8d63291e20efc8d"), CourseBatch("0130334873750159361","2020-06-11","2030-06-30","013016492159606784174"))
    val userCourseDf = userDF.withColumn("course_completion", lit(""))
        .withColumn("l1identifier", lit(""))
        .withColumn("l1completionPercentage", lit(""))
        .withColumnRenamed("batchid","contextid")
        .withColumnRenamed("course_id","courseid")
    batchInfo.map(batches => {
      val reportDf = CourseMetricsJobV2.getReportDF(batches, userCourseDf, alteredUserCourseDf)
      CourseMetricsJobV2.saveReportToBlobStore(batches, reportDf, storageConfig, reportDf.count(), "course-progress-reports/")
    })

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "01303150537737011211"
    val batch2 = "0130334873750159361"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (2)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c7ef3848-bbdb-4219-8344-817d5b8103fa")
    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("f3dd58a4-a56f-4c1d-95cf-3231927a28e9")
    batch2Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
    batch2Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
    batch2Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
    batch2Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)
    /*
    // TODO: Add assertions here
    EmbeddedES.getAllDocuments("cbatchstats-08-07-2018-16-30").foreach(f => {
      f.contains("lastUpdatedOn") should be (true)
    })
    EmbeddedES.getAllDocuments("cbatch").foreach(f => {
      f.contains("reportUpdatedOn") should be (true)
    })
    */

    /*
    val esOutput = JSONUtils.deserialize[ESOutput](EmbeddedES.getAllDocuments("cbatchstats-08-07-2018-16-30").head)
    esOutput.name should be ("Rajesh Kapoor")
    esOutput.id should be ("user012:1002")
    esOutput.batchId should be ("1002")
    esOutput.courseId should be ("do_112726725832507392141")
    esOutput.rootOrgName should be ("MPPS BAYYARAM")
    esOutput.subOrgUDISECode should be ("")
    esOutput.blockName should be ("")
    esOutput.maskedEmail should be ("*****@gmail.com")

  val esOutput_cbatch = JSONUtils.deserialize[ESOutputCBatch](EmbeddedES.getAllDocuments("cbatch").head)
    esOutput_cbatch.id should be ("1004")
    esOutput_cbatch.participantCount should be ("4")
    esOutput_cbatch.completedCount should be ("0")
    */

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  it should "process the filtered batches" in {
    implicit val fc = mock[FrameworkContext]
    val strConfig = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.CourseMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4","contentFilters": {"request": {"filters": {"framework": "TPD"},"sort_by": {"createdOn": "desc"},"limit": 10000,"fields": ["framework", "identifier", "name", "channel"]}},"reportPath": "course-reports/"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Course Dashboard Metrics","deviceMapping": false}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val storageConfig = StorageConfig("local", "", "/tmp/course-metrics")

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    val schema = Encoders.product[UserData].schema
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(userAggDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(contentHierarchyDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)

    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(systemSettingDF)

    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, jobConfig, List())
  }

  it should "test redis and cassandra connections" in {
    implicit val fc = Option(mock[FrameworkContext])
    spark.sparkContext.stop()

    val strConfig = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.CourseMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Course Dashboard Metrics","deviceMapping": false}""".stripMargin
    getReportingSparkContext(JSONUtils.deserialize[JobConfig](strConfig))
    val conf = openSparkSession(JSONUtils.deserialize[JobConfig](strConfig))
    conf.sparkContext.stop()
    spark = getSparkSession()
  }

  it should "run" in {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val contentDF = List(ContentHierarchy("do_1130934466492252161819","""{"ownershipType": ["createdBy"],"copyright": "Sunbird","subject": ["Math"],"downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934466492252161819/report-course-nc_1598321254533_do_1130934466492252161819_1.0_spine.ecar","channel": "b00bc992ef25f1a9a8d63291e20efc8d","organisation": ["Sunbird"],"language": ["English"],"variants": {"online": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934466492252161819/report-course-nc_1598321254814_do_1130934466492252161819_1.0_online.ecar","size": 6916.0},"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934466492252161819/report-course-nc_1598321254533_do_1130934466492252161819_1.0_spine.ecar","size": 78078.0}},"mimeType": "application/vnd.ekstep.content-collection","leafNodes": ["do_1130314841730334721104", "do_1130314849898332161107", "do_1130314847650037761106", "do_1130314845426565121105"],"objectType": "Content","appIcon": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112583978979860480131/artifact/ball2_1536130246933.png","gradeLevel": ["Grade 1"],"collections": [],"children": [{"ownershipType": ["createdBy"],"copyright": "Sunbird","subject": ["Math"],"downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815377_do_1130934418641469441813_1.0_spine.ecar","channel": "b00bc992ef25f1a9a8d63291e20efc8d","organisation": ["Sunbird"],"language": ["English"],"variants": {"online": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815575_do_1130934418641469441813_1.0_online.ecar","size": 4329.0},"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815377_do_1130934418641469441813_1.0_spine.ecar","size": 41777.0}},"mimeType": "application/vnd.ekstep.content-collection","leafNodes": ["do_1130314841730334721104", "do_1130314845426565121105"],"objectType": "Content","appIcon": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130934418641469441813/artifact/06_maths_book_1566813333849.thumb.jpg","gradeLevel": ["Grade 1"],"collections": [],"children": [{"ownershipType": ["createdBy"],"parent": "do_1130934418641469441813","copyright": "Sunbird","code": "do_1130934431450808321814","channel": "b00bc992ef25f1a9a8d63291e20efc8d","language": ["English"],"mimeType": "application/vnd.ekstep.content-collection","idealScreenSize": "normal","createdOn": "2020-08-25T01:58:16.421+0000","objectType": "Content","children": [{"ownershipType": ["createdBy"],"previewUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314841730334721104/artifact/sftbr-04u.pdf","downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314841730334721104/prad-pdf-content-1_1590758122228_do_1130314841730334721104_2.0.ecar","channel": "in.ekstep","questions": [],"language": ["English"],"variants": {"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314841730334721104/prad-pdf-content-1_1590758122675_do_1130314841730334721104_2.0_spine.ecar","size": 943.0}},"mimeType": "application/pdf","usesContent": [],"artifactUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314841730334721104/artifact/sftbr-04u.pdf","contentEncoding": "identity","contentType": "Resource","identifier": "do_1130314841730334721104","audience": ["Learner"],"visibility": "Default","mediaType": "content","itemSets": [],"osId": "org.ekstep.quiz.app","lastPublishedBy": "System","version": 2,"pragma": ["external"],"prevState": "Live","license": "CC BY 4.0","lastPublishedOn": "2020-05-29T13:15:18.492+0000","size": 736557.0,"concepts": [],"name": "prad PDF Content-1","status": "Live","code": "test-Resourcce","prevStatus": "Processing","methods": [],"streamingUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314841730334721104/artifact/sftbr-04u.pdf","idealScreenSize": "normal","createdOn": "2020-05-29T13:02:25.342+0000","contentDisposition": "inline","lastUpdatedOn": "2020-05-29T13:15:17.957+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-05-29T13:15:23.067+0000","dialcodeRequired": "No","lastStatusChangedOn": "2020-05-29T13:15:23.061+0000","os": ["All"],"cloudStorageKey": "content/do_1130314841730334721104/artifact/sftbr-04u.pdf","libraries": [],"pkgVersion": 2.0,"versionKey": "1590758117957","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130314841730334721104/prad-pdf-content-1_1590758122228_do_1130314841730334721104_2.0.ecar","framework": "NCF","compatibilityLevel": 4,"index": 1,"depth": 2,"parent": "do_1130934431450808321814"}, {"ownershipType": ["createdBy"],"previewUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314845426565121105/artifact/sftbr-04u.pdf","downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314845426565121105/prad-pdf-content-2_1590758131556_do_1130314845426565121105_1.0.ecar","channel": "in.ekstep","questions": [],"language": ["English"],"variants": {"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314845426565121105/prad-pdf-content-2_1590758132007_do_1130314845426565121105_1.0_spine.ecar","size": 859.0}},"mimeType": "application/pdf","usesContent": [],"artifactUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314845426565121105/artifact/sftbr-04u.pdf","contentEncoding": "identity","contentType": "Resource","identifier": "do_1130314845426565121105","audience": ["Learner"],"visibility": "Default","mediaType": "content","itemSets": [],"osId": "org.ekstep.quiz.app","lastPublishedBy": "System","version": 2,"pragma": ["external"],"prevState": "Draft","license": "CC BY 4.0","lastPublishedOn": "2020-05-29T13:15:31.543+0000","size": 736471.0,"concepts": [],"name": "prad PDF Content-2","status": "Live","code": "test-Resourcce","prevStatus": "Processing","methods": [],"streamingUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314845426565121105/artifact/sftbr-04u.pdf","idealScreenSize": "normal","createdOn": "2020-05-29T13:03:10.463+0000","contentDisposition": "inline","lastUpdatedOn": "2020-05-29T13:15:31.107+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-05-29T13:15:32.306+0000","dialcodeRequired": "No","lastStatusChangedOn": "2020-05-29T13:15:32.302+0000","os": ["All"],"cloudStorageKey": "content/do_1130314845426565121105/artifact/sftbr-04u.pdf","libraries": [],"pkgVersion": 1.0,"versionKey": "1590758131107","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130314845426565121105/prad-pdf-content-2_1590758131556_do_1130314845426565121105_1.0.ecar","framework": "NCF","compatibilityLevel": 4,"index": 2,"depth": 2,"parent": "do_1130934431450808321814"}],"contentDisposition": "inline","lastUpdatedOn": "2020-08-25T02:00:14.710+0000","contentEncoding": "gzip","contentType": "CourseUnit","dialcodeRequired": "No","identifier": "do_1130934431450808321814","lastStatusChangedOn": "2020-08-25T01:58:16.421+0000","audience": ["Learner"],"os": ["All"],"visibility": "Parent","index": 1,"mediaType": "content","osId": "org.ekstep.launcher","languageCode": ["en"],"versionKey": "1598320696421","license": "CC BY 4.0","idealScreenDensity": "hdpi","framework": "NCFCOPY","depth": 1,"compatibilityLevel": 1,"name": "Unit - RC1","topic": [],"status": "Live","lastPublishedOn": "2020-08-25T02:00:15.228+0000","pkgVersion": 1.0,"leafNodesCount": 2,"leafNodes": ["do_1130314841730334721104", "do_1130314845426565121105"],"downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815377_do_1130934418641469441813_1.0_spine.ecar","variants": "{\"online\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815575_do_1130934418641469441813_1.0_online.ecar\",\"size\":4329.0},\"spine\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934418641469441813/report-course-1_1598320815377_do_1130934418641469441813_1.0_spine.ecar\",\"size\":41777.0}}"}],"appId": "dev.sunbird.portal","contentEncoding": "gzip","lockKey": "eeaa0f71-93de-4be2-9c3c-2e072f0b779b","mimeTypesCount": "{\"application/pdf\":2,\"application/vnd.ekstep.content-collection\":1}","totalCompressedSize": 1473028.0,"contentType": "Course","identifier": "do_1130934418641469441813","lastUpdatedBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","audience": ["Learner"],"toc_url": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130934418641469441813/artifact/do_1130934418641469441813_toc.json","visibility": "Default","contentTypesCount": "{\"CourseUnit\":1,\"Resource\":2}","childNodes": ["do_1130314841730334721104", "do_1130934431450808321814", "do_1130314845426565121105"],"consumerId": "e56ecdfe-e745-49e8-aa53-5edc4996dc8e","mediaType": "content","osId": "org.ekstep.quiz.app","lastPublishedBy": "Ekstep","version": 2,"prevState": "Review","license": "CC BY 4.0","lastPublishedOn": "2020-08-25T02:00:15.228+0000","size": 41777.0,"name": "Report - Course - 1","status": "Live","code": "org.sunbird.xI751R","prevStatus": "Processing","description": "Enter description for Course","medium": ["English"],"posterImage": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112835334818643968148/artifact/06_maths_book_1566813333849.jpg","idealScreenSize": "normal","createdOn": "2020-08-25T01:55:40.060+0000","copyrightYear": 2020,"contentDisposition": "inline","licenseterms": "By creating and uploading content on DIKSHA, you consent to publishing this content under the Creative Commons Framework, specifically under the CC-BY-SA 4.0 license.","lastUpdatedOn": "2020-08-25T02:00:14.710+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-08-25T02:00:15.909+0000","dialcodeRequired": "No","creator": "Creation","createdFor": ["ORG_001"],"lastStatusChangedOn": "2020-08-25T02:00:15.902+0000","os": ["All"],"pkgVersion": 1.0,"versionKey": "1598320814710","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130934418641469441813/report-course-1_1598320815377_do_1130934418641469441813_1.0_spine.ecar","depth": 0,"framework": "NCFCOPY","lastSubmittedOn": "2020-08-25T01:58:52.691+0000","createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","leafNodesCount": 2,"compatibilityLevel": 4,"usedByContent": [],"board": "NCERT","resourceType": "Course","reservedDialcodes": {"T5S3J3": 0},"c_sunbird_dev_private_batch_count": 0,"c_sunbird_dev_open_batch_count": 1,"batches": [{"createdFor": ["ORG_001"],"endDate": null,"name": "Report - Course - 1","batchId": "0130934456323358720","enrollmentType": "open","enrollmentEndDate": null,"startDate": "2020-08-25","status": 1}],"index": 1,"parent": "do_1130934466492252161819"}, {"ownershipType": ["createdBy"],"copyright": "Sunbird","subject": ["Math"],"downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081563_do_1130934445218283521816_1.0_spine.ecar","channel": "b00bc992ef25f1a9a8d63291e20efc8d","organisation": ["Sunbird"],"language": ["English"],"variants": {"online": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081748_do_1130934445218283521816_1.0_online.ecar","size": 4348.0},"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081563_do_1130934445218283521816_1.0_spine.ecar","size": 25416.0}},"mimeType": "application/vnd.ekstep.content-collection","leafNodes": ["do_1130314849898332161107", "do_1130314847650037761106"],"objectType": "Content","appIcon": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11274189945383321612/artifact/51mmhlbsyel._sx258_bo1204203200__1555407648400.jpg","gradeLevel": ["Grade 1"],"collections": [],"children": [{"ownershipType": ["createdBy"],"parent": "do_1130934445218283521816","copyright": "Sunbird","code": "2a9a8e28-757a-4b15-8829-aa7bb0c74096","channel": "b00bc992ef25f1a9a8d63291e20efc8d","language": ["English"],"mimeType": "application/vnd.ekstep.content-collection","idealScreenSize": "normal","createdOn": "2020-08-25T02:03:53.366+0000","objectType": "Content","children": [{"ownershipType": ["createdBy"],"previewUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314847650037761106/artifact/sftbr-04u.pdf","downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314847650037761106/prad-pdf-content-3_1590758142406_do_1130314847650037761106_1.0.ecar","channel": "in.ekstep","questions": [],"language": ["English"],"variants": {"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314847650037761106/prad-pdf-content-3_1590758142803_do_1130314847650037761106_1.0_spine.ecar","size": 859.0}},"mimeType": "application/pdf","usesContent": [],"artifactUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314847650037761106/artifact/sftbr-04u.pdf","contentEncoding": "identity","contentType": "Resource","identifier": "do_1130314847650037761106","audience": ["Learner"],"visibility": "Default","mediaType": "content","itemSets": [],"osId": "org.ekstep.quiz.app","lastPublishedBy": "System","version": 2,"pragma": ["external"],"prevState": "Draft","license": "CC BY 4.0","lastPublishedOn": "2020-05-29T13:15:41.207+0000","size": 736473.0,"concepts": [],"name": "prad PDF Content-3","status": "Live","code": "test-Resourcce","prevStatus": "Processing","methods": [],"streamingUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314847650037761106/artifact/sftbr-04u.pdf","idealScreenSize": "normal","createdOn": "2020-05-29T13:03:37.605+0000","contentDisposition": "inline","lastUpdatedOn": "2020-05-29T13:15:40.816+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-05-29T13:15:43.114+0000","dialcodeRequired": "No","lastStatusChangedOn": "2020-05-29T13:15:43.110+0000","os": ["All"],"cloudStorageKey": "content/do_1130314847650037761106/artifact/sftbr-04u.pdf","libraries": [],"pkgVersion": 1.0,"versionKey": "1590758140816","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130314847650037761106/prad-pdf-content-3_1590758142406_do_1130314847650037761106_1.0.ecar","framework": "NCF","compatibilityLevel": 4,"index": 1,"depth": 2,"parent": "do_1130934459053342721817"}, {"ownershipType": ["createdBy"],"previewUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314849898332161107/artifact/sftbr-04u.pdf","downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314849898332161107/prad-pdf-content-4_1590758137227_do_1130314849898332161107_1.0.ecar","channel": "in.ekstep","questions": [],"language": ["English"],"variants": {"spine": {"ecarUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130314849898332161107/prad-pdf-content-4_1590758137643_do_1130314849898332161107_1.0_spine.ecar","size": 860.0}},"mimeType": "application/pdf","usesContent": [],"artifactUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314849898332161107/artifact/sftbr-04u.pdf","contentEncoding": "identity","contentType": "Resource","identifier": "do_1130314849898332161107","audience": ["Learner"],"visibility": "Default","mediaType": "content","itemSets": [],"osId": "org.ekstep.quiz.app","lastPublishedBy": "System","version": 2,"pragma": ["external"],"prevState": "Draft","license": "CC BY 4.0","lastPublishedOn": "2020-05-29T13:15:32.747+0000","size": 736472.0,"concepts": [],"name": "prad PDF Content-4","status": "Live","code": "test-Resourcce","prevStatus": "Processing","methods": [],"streamingUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130314849898332161107/artifact/sftbr-04u.pdf","idealScreenSize": "normal","createdOn": "2020-05-29T13:04:05.049+0000","contentDisposition": "inline","lastUpdatedOn": "2020-05-29T13:15:32.376+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-05-29T13:15:37.968+0000","dialcodeRequired": "No","lastStatusChangedOn": "2020-05-29T13:15:37.963+0000","os": ["All"],"cloudStorageKey": "content/do_1130314849898332161107/artifact/sftbr-04u.pdf","libraries": [],"pkgVersion": 1.0,"versionKey": "1590758132376","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130314849898332161107/prad-pdf-content-4_1590758137227_do_1130314849898332161107_1.0.ecar","framework": "NCF","compatibilityLevel": 4,"index": 2,"depth": 2,"parent": "do_1130934459053342721817"}],"contentDisposition": "inline","lastUpdatedOn": "2020-08-25T02:04:41.082+0000","contentEncoding": "gzip","contentType": "CourseUnit","dialcodeRequired": "No","identifier": "do_1130934459053342721817","lastStatusChangedOn": "2020-08-25T02:03:53.366+0000","audience": ["Learner"],"os": ["All"],"visibility": "Parent","index": 1,"mediaType": "content","osId": "org.ekstep.launcher","languageCode": ["en"],"versionKey": "1598321033366","license": "CC BY 4.0","idealScreenDensity": "hdpi","framework": "NCFCOPY","depth": 1,"compatibilityLevel": 1,"name": "Unit - RC2","status": "Live","lastPublishedOn": "2020-08-25T02:04:41.384+0000","pkgVersion": 1.0,"leafNodesCount": 2,"leafNodes": ["do_1130314849898332161107", "do_1130314847650037761106"],"downloadUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081563_do_1130934445218283521816_1.0_spine.ecar","variants": "{\"online\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081748_do_1130934445218283521816_1.0_online.ecar\",\"size\":4348.0},\"spine\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_1130934445218283521816/report-course-2_1598321081563_do_1130934445218283521816_1.0_spine.ecar\",\"size\":25416.0}}"}],"appId": "dev.sunbird.portal","contentEncoding": "gzip","lockKey": "37e17366-1efb-46e4-9d6e-8d4c4eb4831a","mimeTypesCount": "{\"application/pdf\":2,\"application/vnd.ekstep.content-collection\":1}","totalCompressedSize": 1472945.0,"contentType": "Course","identifier": "do_1130934445218283521816","lastUpdatedBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","audience": ["Learner"],"toc_url": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130934445218283521816/artifact/do_1130934445218283521816_toc.json","visibility": "Default","contentTypesCount": "{\"CourseUnit\":1,\"Resource\":2}","childNodes": ["do_1130314849898332161107", "do_1130934459053342721817", "do_1130314847650037761106"],"consumerId": "30dd374b-0358-4e55-8869-151712d0211a","mediaType": "content","osId": "org.ekstep.quiz.app","lastPublishedBy": "Ekstep","version": 2,"prevState": "Review","license": "CC BY 4.0","lastPublishedOn": "2020-08-25T02:04:41.384+0000","size": 25416.0,"name": "Report - Course - 2","status": "Live","code": "org.sunbird.gPffCh","prevStatus": "Processing","description": "Enter description for Course","medium": ["English"],"idealScreenSize": "normal","createdOn": "2020-08-25T02:01:04.485+0000","copyrightYear": 2020,"contentDisposition": "inline","licenseterms": "By creating and uploading content on DIKSHA, you consent to publishing this content under the Creative Commons Framework, specifically under the CC-BY-SA 4.0 license.","lastUpdatedOn": "2020-08-25T02:04:41.082+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-08-25T02:04:42.107+0000","dialcodeRequired": "No","creator": "Creation","createdFor": ["ORG_001"],"lastStatusChangedOn": "2020-08-25T02:04:42.099+0000","os": ["All"],"pkgVersion": 1.0,"versionKey": "1598321081082","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130934445218283521816/report-course-2_1598321081563_do_1130934445218283521816_1.0_spine.ecar","depth": 0,"framework": "NCFCOPY","lastSubmittedOn": "2020-08-25T02:04:25.386+0000","createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","leafNodesCount": 2,"compatibilityLevel": 4,"usedByContent": [],"board": "NCERT","resourceType": "Course","reservedDialcodes": {"E6D5Z4": 0},"c_sunbird_dev_private_batch_count": 0,"c_sunbird_dev_open_batch_count": 1,"batches": [{"createdFor": ["ORG_001"],"endDate": null,"name": "Report - Course - 2","batchId": "0130934477547438081","enrollmentType": "open","enrollmentEndDate": null,"startDate": "2020-08-25","status": 1}],"index": 2,"parent": "do_1130934466492252161819"}],"appId": "dev.sunbird.portal","contentEncoding": "gzip","lockKey": "69639168-6f88-44bb-b5a2-ee5bb37640d4","mimeTypesCount": "{\"application/pdf\":4,\"application/vnd.ekstep.content-collection\":4}","totalCompressedSize": 2945973.0,"contentType": "Course","identifier": "do_1130934466492252161819","lastUpdatedBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","audience": ["Learner"],"toc_url": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1130934466492252161819/artifact/do_1130934466492252161819_toc.json","visibility": "Default","contentTypesCount": "{\"CourseUnit\":2,\"Resource\":4,\"Course\":2}","childNodes": ["do_1130314841730334721104", "do_1130314849898332161107", "do_1130934418641469441813", "do_1130934431450808321814", "do_1130934459053342721817", "do_1130314847650037761106", "do_1130314845426565121105", "do_1130934445218283521816"],"consumerId": "10ac6c1a-5775-4cbe-92ab-9f509f888d3d","mediaType": "content","osId": "org.ekstep.quiz.app","lastPublishedBy": "Ekstep","version": 2,"prevState": "Review","license": "CC BY 4.0","lastPublishedOn": "2020-08-25T02:07:34.312+0000","size": 78078.0,"name": "Report - Course - NC","status": "Live","code": "org.sunbird.Lb6NCT","prevStatus": "Processing","description": "Enter description for Course","medium": ["English"],"idealScreenSize": "normal","createdOn": "2020-08-25T02:05:24.176+0000","copyrightYear": 2020,"contentDisposition": "inline","licenseterms": "By creating and uploading content on DIKSHA, you consent to publishing this content under the Creative Commons Framework, specifically under the CC-BY-SA 4.0 license.","lastUpdatedOn": "2020-08-25T02:07:34.045+0000","SYS_INTERNAL_LAST_UPDATED_ON": "2020-08-25T02:07:35.153+0000","dialcodeRequired": "No","creator": "Creation","createdFor": ["ORG_001"],"lastStatusChangedOn": "2020-08-25T02:07:35.145+0000","os": ["All"],"pkgVersion": 1.0,"versionKey": "1598321254045","idealScreenDensity": "hdpi","s3Key": "ecar_files/do_1130934466492252161819/report-course-nc_1598321254533_do_1130934466492252161819_1.0_spine.ecar","depth": 0,"framework": "NCFCOPY","lastSubmittedOn": "2020-08-25T02:07:21.345+0000","createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","leafNodesCount": 4,"compatibilityLevel": 4,"usedByContent": [],"board": "NCERT","resourceType": "Course","reservedDialcodes": {"T7V5X9": 0},"c_sunbird_dev_private_batch_count": 0,"c_sunbird_dev_open_batch_count": 1,"batches": [{"createdFor": ["ORG_001"],"endDate": null,"name": "Report - Course - NC","batchId": "0130934495109529602","enrollmentType": "open","enrollmentEndDate": null,"startDate": "2020-08-25","status": 1}]}""")
    ).toDF()

    val userAgDF = List(UserAgg("Course","do_1130934466492252161819","587204af-41db-4313-b3ab-cf022d3055c6","cb:0130934495109529602",Map("completedCount"->2),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course","do_1130934445218283521816","587204af-41db-4313-b3ab-cf022d3055c6","cb:0130934495109529602",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),

      UserAgg("Course","do_1130934459053342721817","587204af-41db-4313-b3ab-cf022d3055c6","cb:0130934495109529602",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course","do_1130934431450808321814","587204af-41db-4313-b3ab-cf022d3055c6","cb:0130934495109529602",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course","do_1130934418641469441813","587204af-41db-4313-b3ab-cf022d3055c6","cb:0130934495109529602",Map("completedCount"->1),"{'completedCount': '2020-07-21 08:30:48.855000+0000'}")
    ).toDF()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(userAgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(contentDF)

    implicit val sc = spark
    implicit val fc = new FrameworkContext

    val userCourses = CourseMetricsJobV2.getUserCourseInfo(reporterMock.loadData)
    userCourses.show(false)
  }

  it should "exp;ode col" in {

    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val testdf = List(CourseData("do_id","4",List(Level1Data("id_leve1","2"))),
      CourseData("do_id0987654456","1",List[Level1Data]())).toDF()
    testdf.show(false)

    val flattened = testdf.select($"courseid",$"leafNodesCount",$"level1Data", explode_outer($"level1Data").as("exploded_level1Data"))
      .select("courseid","leafNodesCount","exploded_level1Data.*")
//      .select(explode($"rows").as("rows"))
//      .select($"rows".getItem(0).as("idx"),$"rows".getItem(1).as("label"))
    flattened.show()
  }

}