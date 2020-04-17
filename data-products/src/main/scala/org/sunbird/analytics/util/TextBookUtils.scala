package org.sunbird.analytics.util

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, Params}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.model.report._

import scala.util.control.Breaks._
import scala.util.control._

case class ETBTextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel: String,
                           subject: String, status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Int,
                           totalQRLinked: Int, totalQRNotLinked: Int, leafNodesCount: Int, leafNodeUnlinked: Int)
case class DCETextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel:String, subject: String,
                           createdOn: String, lastUpdatedOn: String, totalQRCodes: Int, contentLinkedQR: Int,
                           withoutContentQR: Int, withoutContentT1: Int, withoutContentT2: Int)
case class DialcodeExceptionData(channel: String, identifier: String, medium: String, gradeLevel: String, subject: String, name: String,
                                   l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcode: String,
                                   status: String, nodeType: String, noOfContent: Int, noOfScans: Int, term: String, reportName: String)
case class ContentInformation(id: String, ver: String, ts: String, params: Params, responseCode: String,result: TextbookResult)
case class TextbookResult(count: Int, content: List[TBContentResult])

object TBConstants {
  val textbookunit = "TextBookUnit"
}

object TextBookUtils {

  def getTextBooks(config: Map[String, AnyRef], restUtil: HTTPClient): List[TextBookInfo] = {
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = restUtil.post[TextBookDetails](apiURL, request)
    if(null != response && "successful".equals(response.params.status) && response.result.count>0) response.result.content else List()
  }

  def getTextbookHierarchy(config: Map[String, AnyRef], textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo],restUtil: HTTPClient)(implicit sc: SparkContext, fc: FrameworkContext): (RDD[FinalOutput]) = {
    val reportTuple = for {textbook <- textbookInfo
      baseUrl = s"${AppConf.getConfig("hierarchy.search.api.url")}${AppConf.getConfig("hierarchy.search.api.path")}${textbook.identifier}"
      finalUrl = if("Live".equals(textbook.status)) baseUrl else s"$baseUrl?mode=edit"
      response = RestUtil.get[ContentDetails](finalUrl)
      tupleData = if(null != response && "successful".equals(response.params.status)) {
      val data = response.result.content
        val dceDialcode = generateDCEDialCodeReport(data)
        val dceDialcodeReport = dceDialcode._1
        val etbDialcode = generateETBDialcodeReport(data)
        val etbDialcodeReport = etbDialcode._1
        val etbReport = generateETBTextbookReport(data)
        val dceReport = generateDCETextbookReport(data)
        (etbReport, dceReport, dceDialcodeReport, etbDialcodeReport, dceDialcode._2,etbDialcode._2)
       }
       else (List(),List(),List(),List(),List(),List())
    } yield tupleData
    val etbTextBookReport = reportTuple.filter(f => f._1.nonEmpty).map(f => f._1.head)
    val dceTextBookReport = reportTuple.filter(f => f._2.nonEmpty).map(f => f._2.head)
    val dceDialCodeReport = reportTuple.map(f => f._3).filter(f => f.nonEmpty)
    val dcereport = if(dceDialCodeReport.nonEmpty) dceDialCodeReport.head else List()
    val etbDialCodeReport = reportTuple.map(f => f._4).filter(f => f.nonEmpty)
    val etbreport = if(etbDialCodeReport.nonEmpty) etbDialCodeReport.head else List()
    val dialcodeScans = reportTuple.map(f => f._5).filter(f=>f.nonEmpty) ++ reportTuple.map(f => f._6).filter(f=>f.nonEmpty)
    val scans = dialcodeScans.map(f => f.head)
    val dialcodeReport = dcereport ++ etbreport

//    generateWeeklyScanReport(config, scans)
    val dscans = List(WeeklyDialCodeScans("2020-01-20","DGHEDHH",1.0,"dialcode_scans","dialcode_counts"),WeeklyDialCodeScans("date","T2I6C9",2.0,"dialcode_scans","dialcode_counts"))
    generateWeeklyScanReport(config,dscans)
    generateTextBookReport(sc.parallelize(etbTextBookReport), sc.parallelize(dceTextBookReport), sc.parallelize(dialcodeReport), tenantInfo)
  }

  def generateWeeklyScanReport(config: Map[String, AnyRef], dialcodeScans: List[WeeklyDialCodeScans])(implicit sc: SparkContext, fc: FrameworkContext) {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val configMap = config("dialcodeReportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
    val testconf = Map("reportConfig"-> configMap,"store"->config("store"),"folderPrefix"->config("folderPrefix"),"filePath"->config("filePath"),"container"->config("container"),"format"->config("format"),"key"->config("key"))
    println(testconf)
    val scansDf = sc.parallelize(dialcodeScans).toDF()
    reportConfig.output.map { f =>
      println("f",f)
      CourseUtils.postDataToBlob(scansDf,f,config)
    }
  }

  def generateTextBookReport(etbTextBookReport: RDD[ETBTextbookData], dceTextBookReport: RDD[DCETextbookData], dialcodeReport: RDD[DialcodeExceptionData], tenantInfo: RDD[TenantInfo])(implicit sc: SparkContext): RDD[FinalOutput] = {
    val tenantRDD = tenantInfo.map(e => (e.id,e))
    val etbTextBook = etbTextBookReport.map(e => (e.channel,e))
    val etb=ETBTextbookData("","","","","","","","","",0,0,0,0,0)
    val etbTextBookRDD = etbTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      ETBTextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug, textbook._2._1.getOrElse(etb).identifier,
        textbook._2._1.getOrElse(etb).name,textbook._2._1.getOrElse(etb).medium,textbook._2._1.getOrElse(etb).gradeLevel,
        textbook._2._1.getOrElse(etb).subject,textbook._2._1.getOrElse(etb).status,textbook._2._1.getOrElse(etb).createdOn,textbook._2._1.getOrElse(etb).lastUpdatedOn,
        textbook._2._1.getOrElse(etb).totalContentLinked,textbook._2._1.getOrElse(etb).totalQRLinked,textbook._2._1.getOrElse(etb).totalQRNotLinked,
        textbook._2._1.getOrElse(etb).leafNodesCount,textbook._2._1.getOrElse(etb).leafNodeUnlinked,"ETB_textbook_data")
    })
    val dceTextBook = dceTextBookReport.filter(e => (e.totalQRCodes!=0)).map(e => (e.channel,e))
    val dce = DCETextbookData("","","","","","","","",0,0,0,0,0)
    val dceTextBookRDD = dceTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      DCETextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dce).identifier,
        textbook._2._1.getOrElse(dce).name,textbook._2._1.getOrElse(dce).medium,textbook._2._1.getOrElse(dce).gradeLevel,textbook._2._1.getOrElse(dce).subject,
        textbook._2._1.getOrElse(dce).createdOn,textbook._2._1.getOrElse(dce).lastUpdatedOn,textbook._2._1.getOrElse(dce).totalQRCodes,
        textbook._2._1.getOrElse(dce).contentLinkedQR,textbook._2._1.getOrElse(dce).withoutContentQR,textbook._2._1.getOrElse(dce).withoutContentT1,
        textbook._2._1.getOrElse(dce).withoutContentT2,"DCE_textbook_data")
    })
    val dceRDD = dceTextBookRDD.map(e => (e.identifier,e))
    val etbRDD = etbTextBookRDD.map(e => (e.identifier,e)).fullOuterJoin(dceRDD)

    val dceDialcode = dialcodeReport.map(e => (e.channel,e))
    val dialcode = DialcodeExceptionData("","","","","","","","","","","","","","",0,0,"","")
    val dceDialcodeRDD = dceDialcode.fullOuterJoin(tenantRDD).map(textbook => {
      DialcodeExceptionReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dialcode).identifier,
        textbook._2._1.getOrElse(dialcode).medium,textbook._2._1.getOrElse(dialcode).gradeLevel,textbook._2._1.getOrElse(dialcode).subject,
        textbook._2._1.getOrElse(dialcode).name,textbook._2._1.getOrElse(dialcode).l1Name,textbook._2._1.getOrElse(dialcode).l2Name,textbook._2._1.getOrElse(dialcode).l3Name,
        textbook._2._1.getOrElse(dialcode).l4Name,textbook._2._1.getOrElse(dialcode).l5Name,textbook._2._1.getOrElse(dialcode).dialcode,textbook._2._1.getOrElse(dialcode).status,
        textbook._2._1.getOrElse(dialcode).nodeType,textbook._2._1.getOrElse(dialcode).noOfContent, textbook._2._1.getOrElse(dialcode).noOfScans,textbook._2._1.getOrElse(dialcode).term,textbook._2._1.getOrElse(dialcode).reportName)
    })

    val textbookReports = etbRDD.map(report => FinalOutput(report._1, report._2._1, report._2._2, null))
    val dialcodeReports = dceDialcodeRDD.map(report => FinalOutput(report.identifier, null, null, Option(report)))

    textbookReports.union(dialcodeReports)

  }

  def generateETBDialcodeReport(response: ContentInfo)(implicit sc: SparkContext, fc: FrameworkContext): (List[DialcodeExceptionData],List[WeeklyDialCodeScans]) = {
    var dialcodeReport = List[DialcodeExceptionData]()
    var weeklyDialcodes = List[WeeklyDialCodeScans]()
    val report = DialcodeExceptionData(response.channel,response.identifier,getString(response.medium),getString(response.gradeLevel),getString(response.subject),response.name,"","","","","","",response.status,"",response.leafNodesCount,0,"","ETB_dialcode_data")
    if(null != response && response.children.isDefined) {
//      println("parseETBDialcode",response.identifier)
      val report = parseETBDialcode(response.children.get, response, List[ContentInfo]())
        dialcodeReport = (report._1 ++ dialcodeReport).reverse
        if(report._2.nonEmpty) { weeklyDialcodes = weeklyDialcodes ++ report._2 }
    }
    (report::dialcodeReport,weeklyDialcodes)
  }

  def parseETBDialcode(data: List[ContentInfo], response: ContentInfo, newData: List[ContentInfo], prevData: List[DialcodeExceptionData] = List())(implicit sc: SparkContext, fc: FrameworkContext): (List[DialcodeExceptionData],List[WeeklyDialCodeScans]) = {
    var textbook = List[ContentInfo]()
    var etbDialcode = prevData
    var dialcode = ""

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
          val textbookInfo = getTextBookInfo(textbook)
          val levelNames = textbookInfo._1
          val dialcodeInfo = textbookInfo._2.lift(0).getOrElse("")
          dialcode = dialcodeInfo
          val noOfContents = units.leafNodesCount
          val dialcodes = units.dialcodes
          val nodeType = if(null != noOfContents && noOfContents==0) "Leaf Node" else if(null != dialcodes && dialcodes.nonEmpty) "Leaf Node & QR Linked" else "QR Linked"
          val report = DialcodeExceptionData(response.channel,response.identifier,getString(response.medium),getString(response.gradeLevel), getString(response.subject),response.name,levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),levelNames.lift(4).getOrElse(""), dialcodeInfo,response.status,nodeType,noOfContents,0,"","ETB_dialcode_data")
          etbDialcode = report :: etbDialcode
      }
      else { etbDialcode = parseETBDialcode(units.children.getOrElse(List[ContentInfo]()),response,textbook,etbDialcode)._1 }
    })
    val scans = getDialcodeScans(dialcode)
    (etbDialcode, scans)
  }

  def generateDCEDialCodeReport(response: ContentInfo)(implicit sc: SparkContext, fc: FrameworkContext): (List[DialcodeExceptionData],List[WeeklyDialCodeScans]) = {
    var index=0
    var dialcodeReport = List[DialcodeExceptionData]()
    var weeklyDialcodes = List[WeeklyDialCodeScans]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      response.children.get.map(chapters => {
        val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
        index = index+1
//        println("parseDCEDialcode",response.identifier)
        val report = parseDCEDialcode(chapters.children.getOrElse(List[ContentInfo]()),response,term,chapters.name,List[ContentInfo]())
        dialcodeReport = (report._1 ++ dialcodeReport).reverse
        if(report._2.nonEmpty) { weeklyDialcodes = weeklyDialcodes ++ report._2 }
      })
    }
    (dialcodeReport, weeklyDialcodes)
  }

  def parseDCEDialcode(data: List[ContentInfo], response: ContentInfo, term: String, l1: String, newData: List[ContentInfo], prevData: List[DialcodeExceptionData] = List())(implicit sc: SparkContext, fc: FrameworkContext): (List[DialcodeExceptionData],List[WeeklyDialCodeScans]) = {
    var textbook = List[ContentInfo]()
    var dceDialcode= prevData
    var dialcode = ""

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
        if(null != units.leafNodesCount && units.leafNodesCount == 0) {
          val textbookInfo = getTextBookInfo(textbook)
          val levelNames = textbookInfo._1
          val dialcodes = textbookInfo._2.lift(0).getOrElse("")
          dialcode = dialcodes
          val report = DialcodeExceptionData(response.channel, response.identifier, getString(response.medium), getString(response.gradeLevel),getString(response.subject), response.name, l1,levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),dialcodes,"","",0,0,term,"DCE_dialcode_data")
          dceDialcode = report :: dceDialcode
        }
        else { dceDialcode = parseDCEDialcode(units.children.getOrElse(List[ContentInfo]()),response,term,l1,textbook,dceDialcode)._1 }
      }
    })
    val scans = getDialcodeScans(dialcode)
    (dceDialcode, scans)
  }

  def getDialcodeScans(dialcode: String)(implicit sc: SparkContext, fc: FrameworkContext): List[WeeklyDialCodeScans] = {
    val result= if(dialcode.nonEmpty) {
//      println(dialcode)
      val query = s"""{"queryType": "groupBy","dataSource": "telemetry-events","intervals": "2019-04-09T00:00:00+00:00/2020-04-16T00:00:00+00:00","aggregations": [{"name": "scans","type": "count"}],"dimensions": [{"fieldName": "object_id","aliasName": "dialcode"}],"filters": [{"type": "equals","dimension": "eid","value": "SEARCH"},{"type":"equals","dimension":"object_id","value":"$dialcode"},{"type":"in","dimension":"object_type","values":["DialCode","dialcode","qr","Qr"]}],"postAggregation": [],"descending": "false"}""".stripMargin
      val druidQuery = JSONUtils.deserialize[DruidQueryModel](query)
      val druidResponse = DruidDataFetcher.getDruidData(druidQuery)

      druidResponse.map(f => {
        val report = JSONUtils.deserialize[DialcodeScans](f)
        WeeklyDialCodeScans(report.date,report.dialcode,report.scans,"dialcode_scans","dialcode_counts")
      })
    } else List[WeeklyDialCodeScans]()
    result
  }

  def getTextBookInfo(data: List[ContentInfo]): (List[String],List[String]) = {
    var levelNames = List[String]()
    var dialcodes = List[String]()
    var levelCount = 5
    var parsedData = data(data.size-1)

    breakable {
      while(levelCount > 1) {
        if(TBConstants.textbookunit.equals(parsedData.contentType.getOrElse(""))) {
          if(null != parsedData.dialcodes) { dialcodes = parsedData.dialcodes(0) :: dialcodes }
          levelNames = parsedData.name :: levelNames
        }
        if(parsedData.children.getOrElse(List()).nonEmpty) { parsedData = parsedData.children.get(parsedData.children.size-1) }
        else { break() }
        levelCount = levelCount-1
      }
    }
    (levelNames.reverse,dialcodes)
  }

  def generateDCETextbookReport(response: ContentInfo): List[DCETextbookData] = {
    var index=0
    var dceReport = List[DCETextbookData]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
      index = index+1
      val dceTextbook = parseDCETextbook(response.children.get,term,0,0,0,0,0)
      val totalQRCodes = dceTextbook._2
      val qrLinked = dceTextbook._3
      val qrNotLinked = dceTextbook._4
      val term1NotLinked = dceTextbook._5
      val term2NotLinked = dceTextbook._6
      val medium = getString(response.medium)
      val subject = getString(response.subject)
      val gradeLevel = getString(response.gradeLevel)
      val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
      val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
      val dceDf = DCETextbookData(response.channel,response.identifier, response.name, medium, gradeLevel, subject,createdOn, lastUpdatedOn,totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
      dceReport = dceDf::dceReport
    }
    dceReport
  }

  def parseDCETextbook(data: List[ContentInfo], term: String, counter: Int,linkedQr: Int, qrNotLinked:Int, counterT1:Int, counterT2:Int): (Int, Int, Int, Int, Int, Int) = {
    var counterValue=counter
    var counterQrLinked = linkedQr
    var counterNotLinked = qrNotLinked
    var term1NotLinked = counterT1
    var term2NotLinked = counterT2
    var tempValue = 0
    data.map(units => {
      if(null != units.dialcodes){
        counterValue=counterValue+1
        if(null != units.leafNodesCount && units.leafNodesCount>0) { counterQrLinked=counterQrLinked+1 }
        else {
          counterNotLinked=counterNotLinked+1
          if("T1".equals(term)) { term1NotLinked=term1NotLinked+1 }
          else { term2NotLinked = term2NotLinked+1 }
        }
      }
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        val output = parseDCETextbook(units.children.getOrElse(List[ContentInfo]()),term,counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
        tempValue = output._1
        counterQrLinked = output._3
        counterNotLinked = output._4
        term1NotLinked = output._5
        term2NotLinked = output._6
      }
    })
    (counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
  }

  def generateETBTextbookReport(response: ContentInfo): List[ETBTextbookData] = {
    var textBookReport = List[ETBTextbookData]()
    if(null != response && response.children.isDefined) {
      val etbTextbook = parseETBTextbook(response.children.get,response,0,0,0,0)
      val qrLinkedContent = etbTextbook._1
      val qrNotLinked = etbTextbook._2
      val leafNodeswithoutContent = etbTextbook._3
      val totalLeafNodes = etbTextbook._4
      val medium = getString(response.medium)
      val subject = getString(response.subject)
      val gradeLevel = getString(response.gradeLevel)
      val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
      val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
      val textbookDf = ETBTextbookData(response.channel,response.identifier,response.name,medium,gradeLevel,subject,response.status,createdOn,lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
      textBookReport=textbookDf::textBookReport
    }
    textBookReport
  }

  def parseETBTextbook(data: List[ContentInfo], response: ContentInfo, contentLinked: Int, contentNotLinkedQR:Int, leafNodesContent:Int, leafNodesCount:Int): (Int, Int, Int, Int) = {
    var qrLinkedContent = contentLinked
    var contentNotLinked = contentNotLinkedQR
    var leafNodeswithoutContent = leafNodesContent
    var totalLeafNodes = leafNodesCount
    data.map(units => {
      if(units.children.isEmpty){ totalLeafNodes=totalLeafNodes+1 }
      if(units.children.isEmpty && units.leafNodesCount==0) { leafNodeswithoutContent=leafNodeswithoutContent+1 }
      if(null != units.dialcodes){
        if(null != units.leafNodesCount && units.leafNodesCount>0) { qrLinkedContent=qrLinkedContent+1 }
        else { contentNotLinked=contentNotLinked+1 }
      }
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        val output = parseETBTextbook(units.children.getOrElse(List[ContentInfo]()),response,qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
        qrLinkedContent = output._1
        contentNotLinked = output._2
        leafNodeswithoutContent = output._3
        totalLeafNodes = output._4
      }
    })
    (qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
  }

    def getContentDataList(tenantId: String, unirest: UnirestClient)(implicit sc: SparkContext): TextbookResult = {
    implicit val sqlContext = new SQLContext(sc)
    val url = Constants.COMPOSITE_SEARCH_URL
    val request = s"""{
                     |      "request": {
                     |        "filters": {
                     |           "status": ["Live","Draft","Review","Unlisted"],
                     |          "contentType": ["Resource"],
                     |          "createdFor": "$tenantId"
                     |        },
                     |        "fields": ["channel","identifier","board","gradeLevel",
                     |          "medium","subject","status","creator","lastPublishedOn","createdFor",
                     |          "createdOn","pkgVersion","contentType",
                     |          "mimeType","resourceType", "lastSubmittedOn"
                     |        ],
                     |        "limit": 10000,
                     |        "facets": [
                     |          "status"
                     |        ]
                     |      }
                     |    }""".stripMargin
    val header = new util.HashMap[String, String]()
    header.put("Content-Type", "application/json")
    val response = unirest.post(url, request, Option(header))
    JSONUtils.deserialize[ContentInformation](response).result
  }

  def getString(data: Object): String = {
    if (null != data) {
      if (data.isInstanceOf[String]) data.asInstanceOf[String]
      else data.asInstanceOf[List[String]].mkString(",")
    } else ""
  }
}
