package org.sunbird.analytics.job.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext}
import org.ekstep.analytics.framework.util.CommonUtil
import org.sunbird.cloud.storage.conf.AppConf

trait BaseReportsJob {

  def loadData(spark: SparkSession, settings: Map[String, String], schema: Option[StructType] = None): DataFrame = {
    val dataFrameReader = spark.read.format("org.apache.spark.sql.cassandra").options(settings)
    if (schema.nonEmpty) {
      schema.map(schema => dataFrameReader.schema(schema)).getOrElse(dataFrameReader).load()
    } else {
      dataFrameReader.load()
    }

  }

  def getReportingFrameworkContext()(implicit fc: Option[FrameworkContext]): FrameworkContext = {
    fc match {
      case Some(value) => {
        value
      }
      case None => {
        new FrameworkContext();
      }
    }
  }

  def getReportingSparkContext(config: JobConfig)(implicit sc: Option[SparkContext] = None): SparkContext = {

    val sparkContext = sc match {
      case Some(value) => {
        value
      }
      case None => {
        val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
        val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
        val sparkRedisConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkRedisConnectionHost")
        val sparkUserDbRedisIndex = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkUserDbRedisIndex")
        CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, sparkRedisConnectionHost, sparkUserDbRedisIndex)
      }
    }
    setReportsStorageConfiguration(sparkContext)
    sparkContext;
//val conf = new SparkConf().setAppName("AnalyticsTestSuite").set("spark.default.parallelism", "2");
//    conf.set("spark.sql.shuffle.partitions", "2")
//    conf.setMaster("local[*]")
//    conf.set("spark.driver.memory", "1g")
//    conf.set("spark.memory.fraction", "0.3")
//    conf.set("spark.memory.storageFraction", "0.5")
//    conf.set("spark.cassandra.connection.host", "localhost")
//    conf.set("spark.cassandra.connection.port", "9042")
//    conf.set("es.nodes", "http://localhost")
//    conf

//    SparkSession.builder.config(conf).getOrCreate().sparkContext

  }

  def openSparkSession(config: JobConfig): SparkSession = {

    val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
    val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
    val sparkRedisConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkRedisConnectionHost")
    val sparkUserDbRedisIndex = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkUserDbRedisIndex")

    val readConsistencyLevel = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, Option(readConsistencyLevel), sparkRedisConnectionHost, sparkUserDbRedisIndex)
    setReportsStorageConfiguration(sparkSession.sparkContext)
    sparkSession;

  }

  def closeSparkSession()(implicit sparkSession: SparkSession) {
    sparkSession.stop();
  }

  def setReportsStorageConfiguration(sc: SparkContext) {
    val reportsStorageAccountKey = AppConf.getConfig("dock_reports_account_name")
    val reportsStorageAccountSecret = AppConf.getConfig("dock_reports_account_key")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + reportsStorageAccountKey + ".blob.core.windows.net", reportsStorageAccountSecret)
      sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + reportsStorageAccountKey + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    }
  }

  def getStorageConfig(container: String, key: String): org.ekstep.analytics.framework.StorageConfig = {
    val reportsStorageAccountKey = AppConf.getConfig("reports_storage_key")
    val reportsStorageAccountSecret = AppConf.getConfig("reports_storage_secret")
    val provider = AppConf.getConfig("cloud_storage_type")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option("reports_storage_key"), Option("reports_storage_secret"));
    } else {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option(provider), Option(provider));
    }

  }

}