package org.sunbird.analytics.sourcing

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{BaseSpec, SparkSpec}

class TestContentDetailsReport extends SparkSpec with Matchers with MockFactory {
  var spark: SparkSession = _

  it should "generate report" in {
    val config = """{"tenantId": "01309282781705830427","slug": "test-slug","search": {"type": "none"},"model": "org.ekstep.analytics.sourcing.ContentDetailsReport","modelParams": {"reportConfig": {"id": "textbook_report","metrics": [],"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "azure","storageContainer": "'$reportPostContainer'","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "'$reportPostContainer'","sparkCassandraConnectionHost": "'$sunbirdPlatformCassandraHost'","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Content Details Job","deviceMapping": false}""".stripMargin
    ContentDetailsReport.main(config)

  }

}
