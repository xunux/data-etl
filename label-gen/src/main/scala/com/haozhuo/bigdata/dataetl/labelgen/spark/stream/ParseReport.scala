package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import java.sql.{SQLException, PreparedStatement, Connection}
import java.util
import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.haozhuo.bigdata.dataetl.bean.{Report, DBJson}
import com.haozhuo.bigdata.dataetl.hbase._
import com.haozhuo.bigdata.dataetl.mysql.DataSource
import com.haozhuo.bigdata.dataetl.{ScalaUtils, Props, JavaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory
import scala.collection.mutable.{ArrayBuffer}

object ParseReport extends Serializable {

  def objToJson(obj: util.HashMap[String, String]): String = {
    s"""{${jsonField("report_content_id", obj)},${jsonField("health_report_id", obj)},"report_content":${JavaUtils.replaceLineBreak(JavaUtils.replaceLineBreak(obj.get("report_content")))},${jsonField("create_time", obj)},${jsonField("last_update_time", obj)}}"""
  }

  private def jsonField(name: String, obj: util.HashMap[String, String]): String = {
    s""""$name":"${obj.get(name)}""""
  }

  def removeBlank(a: Any): String = if (a == null) "" else a.toString

  def cutSummary(any: Any) = {
    val summary = ScalaUtils.toString(any)
    if (summary.length < 16) {
      summary
    } else {
      summary.substring(0, 15)
    }
  }
}

class ParseReport extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())

  private def parseIndexProcess(jsonDF: DataFrame, sqlContext: SQLContext): Dataset[Row] = {
    import sqlContext.implicits._
    jsonDF.withColumn("checkItem", explode($"reportContent.checkItems"))
      .select("healthReportId", "checkItem.checkItemName", "checkItem.departmentName", "checkItem.checkUserName", "checkItem.checkResults"
      ).withColumn("checkResult", explode($"checkResults"))
      .select("healthReportId", "checkItemName", "checkUserName", "checkResult.checkIndexName", "checkResult.resultValue",
        "checkResult.unit", "checkResult.textRef", "checkResult.resultFlagId", "checkResult.canExplain")
  }

  private def parseSummaryProcess(jsonDF: DataFrame, sqlContext: SQLContext): Dataset[Row] = {
    import sqlContext.implicits._
    jsonDF.withColumn("summary", explode($"reportContent.generalSummarys2"))
      .select("healthReportId", "summary")
      .map(r => (ScalaUtils.toString(r(0)), JavaUtils.replaceLineBreak(ScalaUtils.toString(r(1)))))
      .toDF("healthReportId", "summary")
  }

  private def parseUserProcess(jsonDF: DataFrame, sqlContext: SQLContext): Dataset[Row] = {
    jsonDF.select("healthReportId", "userId", "idCardNoMd5", "birthday", "sex", "checkUnitCode", "checkUnitName", "checkDate", "lastUpdateTime")
  }


  def getLabelsRDD(indexDF: DataFrame, summaryDF: DataFrame, userDF: DataFrame): RDD[(String, Map[String, String])] = {
    val indexLabels = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), LabelRules.genIndexLabel(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2)))))
    val summaryLabels = summaryDF.select("healthReportId", "summary").rdd
      .map(r => (ScalaUtils.toString(r(0)), LabelRules.genSummaryLabel(ScalaUtils.toString(r(1)))))
    val labelRDD = indexLabels.union(summaryLabels).distinct().groupByKey().map(x => (x._1, LabelRules.getDiseases(x._2)))
    val height = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), LabelRules.genHeight(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    val weight = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), LabelRules.genWeight(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    val waistline = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), LabelRules.genWaistline(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    //userRDD.rdd: ("healthReportId","userId","idCardNoMd5", "birthday", "sex", "checkUnitCode", "checkUnitName", "checkDate", "lastUpdateTime")
    val userRDD = userDF.rdd.map(r => (ScalaUtils.toString(r(0)), (ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2)), ScalaUtils.toString(r(3)), ScalaUtils.toString(r(4)), ScalaUtils.toString(r(5)), ScalaUtils.toString(r(6)), ScalaUtils.toString(r(7)), ScalaUtils.toString(r(8)))))
    //(2515218,(CompactBuffer((CompactBuffer(类风湿性关节炎,阴道炎,超重),CompactBuffer(150-170),CompactBuffer(65-90),CompactBuffer())),CompactBuffer((761b9c2e-81f6-4f79-850a-bd112988823d,1163ed15c5f3c149fb1167bc018636d1,2017-01-01,女,美年大健康太原长风分院,美年大健康太原长风分院,2017-01-01 11:11:11,2017-12-12))))
    labelRDD.cogroup(height, weight, waistline).cogroup(userRDD)
      .flatMap {
        x =>
          val arrayBuffer = new ArrayBuffer[(String, Map[String, String])]()
          val userInfo = x._2._2
          if (!userInfo.isEmpty) {
            for ((userId, idCardNoMd5, birthday, sex, checkUnitCode, checkUnitName, checkDate, lastUpdateTime) <- userInfo) {
              val healthReportId = x._1
              val label = if (x._2._1.isEmpty || x._2._1.head._1.isEmpty) "" else x._2._1.head._1.head
              val height = if (x._2._1.isEmpty || x._2._1.head._2.isEmpty) "" else x._2._1.head._2.head
              val weight = if (x._2._1.isEmpty || x._2._1.head._3.isEmpty) "" else x._2._1.head._3.head
              val waistline = if (x._2._1.isEmpty || x._2._1.head._4.isEmpty) "" else x._2._1.head._4.head
              arrayBuffer.+=((healthReportId + "_" + userId, Map("lastUpdateTime" -> lastUpdateTime,
                "healthReportId" -> healthReportId,
                "userId" -> userId,
                "label" -> label,
                "height" -> height,
                "weight" -> weight,
                "waistline" -> waistline,
                "idCardNoMd5" -> idCardNoMd5,
                "birthday" -> birthday,
                "sex" -> sex,
                "checkUnitCode" -> checkUnitCode,
                "checkUnitName" -> checkUnitName,
                "checkDate" -> checkDate,
                "lastUpdateTime" -> lastUpdateTime,
                "labelCreateTime" -> JavaUtils.getStrDate)))
            }
          }
          arrayBuffer
      }
  }

  def labelsToES(labelRDD: RDD[(String, Map[String, String])]) = {
    logger.info("将解析完的报告存入ES中")
    EsSpark.saveToEsWithMeta(labelRDD, s"${Props.get("es.resource")}")

  }

  def labelsToHBase(labelRDD: RDD[(String, Map[String, String])]) = {
    val array = labelRDD.flatMap {
      x =>
        val updateTime = JavaUtils.getStrDate
        x._2.get("label").getOrElse("").split(",").map {
          singleLabel =>
            HBaseRow(
              s"${x._1}_$singleLabel",
              Array(
                HBaseColumn("rpt_id", x._2.get("healthReportId").getOrElse("")),
                HBaseColumn("uid", x._2.get("userId").getOrElse("")),
                HBaseColumn("label", singleLabel),
                HBaseColumn("kw", ""),
                HBaseColumn("bp1", ""),
                HBaseColumn("bp2", ""),
                HBaseColumn("desc", ""),
                HBaseColumn("cls", ""),
                HBaseColumn("upd_t", updateTime)
              ).filter(_.getValue != "")
                array
            )
        }

    }.collect()
    logger.info("将数据保存到HBase的DATAETL:RPT_LABEL中")
    new HBaseClient().putInto(new HBaseTable("DATAETL:RPT_LABEL", array))
  }

  def indexToHBase(indexDF: DataFrame, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val updateTime = JavaUtils.getStrDate()
    val array = indexDF.map {
      r =>
        HBaseRow(ScalaUtils.toString(r(0)) + "_" + ScalaUtils.toString(r(1)),
          Array(
            HBaseColumn("rpt_id", ScalaUtils.toString(r(0))),
            HBaseColumn("chk_item", ScalaUtils.toString(r(1))),
            HBaseColumn("chk_user", ScalaUtils.toString(r(2))),
            HBaseColumn("chk_ind", ScalaUtils.toString(r(3))),
            HBaseColumn("rs_val", ScalaUtils.toString(r(4))),
            HBaseColumn("unit", ScalaUtils.toString(r(5))),
            HBaseColumn("text_ref", ScalaUtils.toString(r(6))),
            HBaseColumn("rs_flag_id", ScalaUtils.toString(r(7))),
            HBaseColumn("exp", ScalaUtils.toString(r(8))),
            HBaseColumn("upd_t", updateTime)
          ).filter(_.getValue != ""))
    }.collect()
    logger.info("将数据保存到HBase的DATAETL:RPT_IND中")
    new HBaseClient().putInto(new HBaseTable("DATAETL:RPT_IND", array))
  }

  def summaryToHBase(jsonDF: DataFrame, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val updateTime = JavaUtils.getStrDate()
    val array = jsonDF.map {
      r =>
        HBaseRow(
          ScalaUtils.toString(r(0)) + "_" + ParseReport.cutSummary(r(1)),
          Array(
            HBaseColumn("rpt_id", ScalaUtils.toString(r(0))),
            HBaseColumn("sum", ScalaUtils.toString(r(1))),
            HBaseColumn("upd_t", updateTime)
          ).filter(_.getValue != ""))
    }.collect()
    logger.info("将数据保存到HBase的DATAETL:RPT_SUM中")
    new HBaseClient().putInto(new HBaseTable("DATAETL:RPT_SUM", array))
  }

  def suggestsToHBase(jsonDF: DataFrame, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    try {
      val updateTime = JavaUtils.getStrDate()
      val array = jsonDF.withColumn("suggest", explode($"reportContent.generalSummarys"))
        .select("healthReportId", "suggest.summaryName", "suggest.summaryMedicalExplanation", "suggest.summaryReasonResult",
          "suggest.summaryAdvice", "suggest.summaryDescription", "suggest.reviewAdvice",
          "suggest.result", "suggest.fw").map {
        r =>
          HBaseRow(
            ScalaUtils.toString(r(0)) + "_" + ScalaUtils.toString(r(1)),
            Array(
              HBaseColumn("rpt_id", ScalaUtils.toString(r(0))),
              HBaseColumn("sug_name", ScalaUtils.toString(r(1))),
              HBaseColumn("sug_med_exp", ScalaUtils.toString(r(2))),
              HBaseColumn("sug_rsn_rs", ScalaUtils.toString(r(3))),
              HBaseColumn("sug_adv", ScalaUtils.toString(r(4))),
              HBaseColumn("sug_desc", ScalaUtils.toString(r(5))),
              HBaseColumn("rev_adv", ScalaUtils.toString(r(6))),
              HBaseColumn("rs", ScalaUtils.toString(r(7))),
              HBaseColumn("fw", ScalaUtils.toString(r(8))),
              HBaseColumn("upd_t", updateTime)
            ).filter(_.getValue != "")
          )
      }.collect()
      logger.info("将数据保存到HBase的DATAETL:RPT_SUG中")
      new HBaseClient().putInto(new HBaseTable("DATAETL:RPT_SUG", array))
    } catch {
      case ex: Exception =>
    }

  }

  def saveToEsHBase(jsonDF: DataFrame, sqlContext: SQLContext) = {
    val indexDF = parseIndexProcess(jsonDF, sqlContext).persist() //尝试不进行缓存
    val summaryDF = parseSummaryProcess(jsonDF, sqlContext).persist()
    val userDF = parseUserProcess(jsonDF, sqlContext)
    val labelRDD = getLabelsRDD(indexDF, summaryDF, userDF).persist()
    labelsToES(labelRDD)
    labelsToHBase(labelRDD)
    indexToHBase(indexDF, sqlContext)
    summaryToHBase(summaryDF, sqlContext)
    suggestsToHBase(jsonDF, sqlContext)
  }

  def reportToHBase(reports: Array[Report]) = {
    val mapper = new ObjectMapper()
    val array = reports.map(
      r =>
        HBaseRow(
          r.getHealthReportId + "_" + r.getUserId,
          Array(
            HBaseColumn("rpt_id", ScalaUtils.toString(r.getHealthReportId)),
            HBaseColumn("uid", ScalaUtils.toString(r.getUserId)),
            HBaseColumn("bd", ScalaUtils.toString(r.getBirthday)),
            HBaseColumn("chk_date", ScalaUtils.toString(r.getCheckDate)),
            HBaseColumn("chk_u_code", ScalaUtils.toString(r.getCheckUnitCode)),
            HBaseColumn("id_card", ScalaUtils.toString(r.getIdCardNoMd5)),
            HBaseColumn("lup_t", ScalaUtils.toString(r.getLastUpdateTime)),
            HBaseColumn("chk_u_name", ScalaUtils.toString(r.getCheckUnitName)),
            HBaseColumn("cont", mapper.writeValueAsString(r.getReportContent)),
            HBaseColumn("sex", r.getSex),
            HBaseColumn("upd_t", r.getUpdateTime)
          ).filter(_.getValue != "")
        )
    )
    logger.info("将数据保存到HBase的DATAETL:RPT中")
    new HBaseClient().putInto(new HBaseTable("DATAETL:RPT", array))
  }

  def saveReport(rdd: RDD[String]): Unit = {
    try {
      val updateTime = JavaUtils.getStrDate()
      val mapper = new ObjectMapper()
      val reportType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[Report])
      val array = rdd.map(x => mapper.readValue(x, reportType).asInstanceOf[DBJson[Report]])
        .map {
          x =>
            val obj = x.getObj;
            obj.setUpdateTime(updateTime);
            obj
        }.collect()
      logger.info("{}条数据", array.length)
      reportToMysql(array)
      logger.info("将{}条report保存到Mysql中完成", array.length)
      reportToHBase(array)
    } catch {
      case e: Exception =>
        logger.error("report出错了", e)
    }
  }

  def reportToMysql(reports: Array[Report]) {
    if (reports.length == 0) return
    val mapper: ObjectMapper = new ObjectMapper
    var conn: Connection = null
    var preparedStmt: PreparedStatement = null
    val query: String = "INSERT INTO report (health_report_id,user_id, id_card_no_md5, birthday, sex, check_unit_code, check_unit_name, report_content, check_date,last_update_time,upd_t) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?,?,?) ON DUPLICATE KEY UPDATE id_card_no_md5=?, birthday=?, sex=?, check_unit_code=?, check_unit_name=?, report_content=?, check_date=?,last_update_time=?"
    try {
      conn = DataSource.getInstance.getConnection
      preparedStmt = conn.prepareStatement(query)
      conn.setAutoCommit(false)
      var i: Int = 0
      while (i < reports.length) {
        val report: Report = reports(i)
        preparedStmt.setLong(1, report.getHealthReportId)
        preparedStmt.setString(2, report.getUserId)
        preparedStmt.setString(3, report.getIdCardNoMd5)
        preparedStmt.setString(4, report.getBirthday)
        preparedStmt.setString(5, report.getSex)
        preparedStmt.setString(6, report.getCheckUnitCode)
        preparedStmt.setString(7, report.getCheckUnitName)
        preparedStmt.setString(8, mapper.writeValueAsString(report.getReportContent))
        preparedStmt.setString(9, report.getCheckDate)
        preparedStmt.setString(10, report.getLastUpdateTime)
        preparedStmt.setString(11, report.getUpdateTime)
        preparedStmt.setString(12, report.getIdCardNoMd5)
        preparedStmt.setString(13, report.getBirthday)
        preparedStmt.setString(14, report.getSex)
        preparedStmt.setString(15, report.getCheckUnitCode)
        preparedStmt.setString(16, report.getCheckUnitName)
        preparedStmt.setString(17, mapper.writeValueAsString(report.getReportContent))
        preparedStmt.setString(18, report.getCheckDate)
        preparedStmt.setString(19, report.getLastUpdateTime)
        preparedStmt.addBatch
        i += 1
      }
      preparedStmt.executeBatch
      conn.commit
      conn.setAutoCommit(true)
    } catch {
      case e: Exception => logger.info("Error", e)
    } finally {
      if (preparedStmt != null) {
        try {
          preparedStmt.close
        }
        catch {
          case e: SQLException => logger.info("Error", e)
        }
      }
      if (conn != null) {
        try {
          conn.close
        } catch {
          case e: SQLException => logger.info("Error", e)
        }
      }
    }
  }
}
