package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import java.util


import com.haozhuo.bigdata.dataetl.hive.HiveCatalog
import com.haozhuo.bigdata.dataetl.{ScalaUtils, Props, JavaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object ParseReport extends Serializable {
  private val gysz = ".*(甘油三[脂酯]).*".r
  private val zdgc = ".*(总胆固醇).*".r
  private val dmdzdb = ".*(低密度脂蛋白).*".r
  private val ssy = ".*(收缩压).*".r
  private val szy = ".*(舒张压).*".r
  private val fp = ".*([Bb][Mm][Ii]|体重指数|身体质量).*".r
  private val gnstf = ".*(尿酸)[^碱]*".r
  private val dmyh = ".*动脉(硬化粥样|粥样硬化).*".r
  private val ggnyc = ".*(谷丙转氨酶|丙氨酸氨基转移酶|谷草转氨酶|天门冬氨酸氨基转移酶|谷氨酰).*".r
  private val zdhs = ".*(总胆红素).*".r
  private val xt = "(?!化血红蛋白).*(血糖)(?!化血红蛋白).*".r
  private val thxhdb = "(?!平均).*(糖化血红蛋白)(?!平均).*".r
  private val fcxc = "(血沉.*方程.*|方程.*血沉.*)".r
  private val cfydb = ".*(C反应蛋白).*".r
  private val ymlxgjgr = ".*(碳14呼气试验|幽门螺旋杆菌抗体检测).*".r
  private val ygbmkt = "(.*乙肝.*表面抗体.*)".r
  private val yghxkt = "(.*乙肝.*核心抗体.*)".r
  private val ygbmky = "(.*乙肝.*表面抗原.*)".r
  private val ygeky = "(.*乙肝.*e抗原.*)".r
  private val ygekt = "(.*乙肝.*e抗体.*)".r
  private val afp = "(.*[Aa][Ff][Pp].*)".r
  private val cea = "(.*[Cc][Ee][Aa].*)".r
  private val crp = "((?!超敏).*[Cc][Rr][Pp](?!超敏).*)".r
  private val bx = "(.*便血.*)".r
  private val yx = "(.*隐血.*)".r
  private val jg = "(.*肌酐.*)".r
  private val ndb = "(.*尿蛋白.*)".r
  private val nwlwdb = "(.*尿微量白蛋白.*)".r
  private val jxbgr = "(.*巨细胞病毒[Ii][gG][Mm]抗体.*)".r


  def genHeight(indexName: String, resultValue: String): String = {
    var result = ""
    if (indexName.contains("身高")) {
      val v = JavaUtils.toDouble(resultValue)
      if (v <= 150) result = "<=150"
      else if (v <= 170 && v > 150) result = "150-170"
      else if (v <= 190 && v > 170) result = "170-190"
      else if (v > 190) result = ">190"
    }
    result
  }

  def genWeight(indexName: String, resultValue: String): String = {
    var result = ""
    if (indexName.contains("体重") && !indexName.contains("理想体重") && !indexName.contains("体重指数")) {
      val v = JavaUtils.toDouble(resultValue)
      if (v <= 40) result = "<=40"
      else if (v <= 65 && v > 40) result = "40-65"
      else if (v <= 90 && v > 65) result = "65-90"
      else if (v > 90) result = ">90"
    }
    result
  }

  def genWaistline(indexName: String, resultValue: String): String = {
    var result = ""
    if (indexName.contains("腰围")) {
      val v = JavaUtils.toDouble(resultValue)
      if (v <= 60) result = "<=60"
      else if (v <= 90 && v > 60) result = "60-90"
      else if (v <= 120 && v > 90) result = "90-120"
      else if (v > 120) result = ">120"
    }
    result
  }

  def genIndexLabel(indexName: String, resultValue: String): String = {
    val v = JavaUtils.toDouble(resultValue)
    indexName match {
      case gysz(c) => if (v >= 2.26) "高血脂" else ""
      case zdgc(c) => if (v >= 6.22) "高血脂" else ""
      case dmdzdb(c) => if (v >= 4.14) "高血脂" else ""
      case ssy(c) => if (v >= 140) "高血压" else if (v < 90) "血压偏低" else ""
      case szy(c) => if (v >= 90) "高血压" else if (v < 60) "血压偏低" else ""
      case fp(c) =>
        if (v >= 40) "重度肥胖"
        else if (v >= 35 && v < 40) "中度肥胖"
        else if (v < 35 && v >= 30) "轻度肥胖"
        else if (v < 30 && v >= 24) "超重"
        else if (v < 18) "偏瘦"
        else ""
      case gnstf(c) => if (v > 420) "高尿酸/痛风" else ""
      case dmyh(c) => if (v >= 4) "动脉硬化" else ""
      case ggnyc(c) => if (v >= 45) "肝功能异常" else ""
      case zdhs(c) => if (v >= 21) "肝功能异常" else ""
      case xt(c) =>
        if (v >= 7)
          "糖尿病"
        else if (v >= 6.2 && v < 7)
          "糖尿病前期"
        else ""
      case thxhdb(c) =>
        if (v >= 6.5)
          "糖尿病"
        else if (v >= 5.5 && v <= 6.5)
          "糖尿病前期"
        else ""
      case fcxc(c) => if (v >= 28) "类风湿性关节炎" else ""
      case cfydb(c) => if (v > 3) "类风湿性关节炎" else ""
      case ymlxgjgr(c) => if (resultValue == "阳性") "幽门螺旋杆菌感染" else ""
      case ygbmky(c) => if (resultValue == "阳性") "乙肝表面抗原" else ""
      case yghxkt(c) => if (resultValue == "阳性") "乙肝核心抗体" else ""
      case ygbmkt(c) => if (resultValue == "阳性") "乙肝表面抗体" else ""
      case ygeky(c) => if (resultValue == "阳性") "乙肝e抗原" else ""
      case ygekt(c) => if (resultValue == "阳性") "乙肝e抗体" else ""
      case afp(c) => if (v > 20) "妊娠期或者肝癌可能" else ""
      case cea(c) => if (v > 10) "肿瘤可能" else ""
      case crp(c) => if (v > 10) "急性炎症/组织损伤" else ""
      case bx(c) => if (resultValue == "有") "消化道出血" else ""
      case yx(c) => if (resultValue == "阳性") "消化道出血" else ""
      case jg(c) => if (v > 115) "肾功能不全" else ""
      case ndb(c) => if (resultValue == "阳性") "肾功能不全" else ""
      case nwlwdb(c) => if (resultValue == "阳性") "肾功能不全" else ""
      case jxbgr(c) => if (resultValue == "阳性") "巨细胞病毒感染中" else ""
      case _ => ""
    }
  }

  private val js = "(?!既往).*(屈光不正)(?!既往).*|.*零视力.*|.*双高度近视.*".r
  private val mnxjs = ".*(肾结石|肾结晶|肾多发结|输尿管结石).*".r
  private val zfgqq = ".*(脂肪肝趋势|肝脂肪浸润|脂肪肝倾向|肝脏脂肪性改变).*".r
  private val zfg = "(.*脂肪肝.*)".r
  private val yg = "(.*隐睾(?!术后).*)".r
  private val zfl = "((?!考虑).*脂肪瘤(?!考虑).*)".r
  private val erdjz = "(.*外耳道疖肿.*)".r
  private val bltjx = "(.*玻璃体积血.*)".r
  private val sezcky = "((?!史).*十二指肠球部溃疡(?!史).*)".r
  private val jxnz = "(.*腱鞘囊肿.*)".r
  private val fwz = "(.*飞蚊症.*)".r
  private val jsjmqz = "(?!术后).*精索静脉曲张(?!术后).*".r
  private val sjxel = "(.*神经性耳聋.*)".r
  private val gwjy = "(.*睾丸鞘膜积液.*)".r
  private val gzcw = "(.*脊柱侧弯.*)".r
  private val gmck = "((?!愈合).*鼓膜穿孔(?!愈合).*)".r
  private val gmsz = ".*(结膜水肿|结膜炎).*".r
  private val rs = "(.*弱视.*)".r
  private val sy = "(.*沙眼.*)".r
  private val gjnsnz = ".*(宫颈纳氏囊肿|宫颈多发纳氏囊肿|宫颈囊肿|宫颈纳囊).*".r
  private val gjml = "(.*宫颈糜烂.*)".r
  private val dnqs = "(.*耵聍栓塞.*)".r
  private val qtdxnz = "(.*前庭大腺囊肿.*)".r
  private val bj = "(.*包茎.*)".r
  private val xzjmqz = "(.*下肢静脉曲张.*)".r
  private val bzgp = "(.*鼻中隔偏曲.*)".r
  private val bdf = "(.*白癜风.*)".r
  private val bnz = "(.*白内障.*)".r
  private val qlx = "(.*前列腺增.*|前列腺.*度.*增.*|.*前列腺稍大伴多发钙化灶.*)".r
  private val ydy = ".*(阴道炎|阴道病|外阴阴道酵母菌病|分泌物.*豆渣样.*|豆渣样.*分泌物.*).*".r
  private val pfgr = ".*(体癣|头癣|皮癣|股癣).*".r
  private val gzjb = ".*(肛裂|肛瘘|外痔|混合痔).*".r
  private val fbx = ".*(腹股沟疝气|腹股沟斜疝|腹壁切口疝).*".r
  private val sgnbq = "(.*血尿素氮.*偏高.*|.*偏高.*血尿素氮.*|.*β2-微球蛋白.*偏高.*|.*偏高.*β2-微球蛋白.*|.*β2-微球蛋白.*增高.*|.*增高.*β2-微球蛋白.*)".r
  private val slqj = ".*(视力欠佳|视力下降|视力严重缺陷|眼科检查.*常见症状，视力减退.*).*".r
  private val lxxyzs = ".*(乳腺小叶增生).*".r
  private val jzxjj = ".*(甲状腺结节).*".r
  private val yjs = ".*(牙结石).*".r
  private val mxyy = ".*(慢性咽炎).*".r
  private val myzk = ".*(脉压增|脉压差增|脉压差大).*".r
  private val bdjjd = ".*(白带清洁度偏高|白带清洁度高).*".r
  private val qlxgh = ".*(前列腺钙化).*".r
  private val dxxdkh = "(窦性.*心动过缓.*)".r
  private val lbxbbfl = ".*(淋巴细胞百分率增|淋巴细胞百分率升|淋巴细胞百分率偏高).*".r
  private val dxlbq = ".*(窦性心律不齐).*".r
  private val gljs = ".*(骨量减少|骨量匮乏|骨量少).*".r
  private val zxlbfl = ".*(中性粒细胞百分率降低|中性粒细胞百分率减少|中性粒细胞百分率偏低).*".r

  def genSummaryLabel(summary: String): String = {
    summary match {
      case js(c) => "近视"
      case mnxjs(c) => "泌尿系结石"
      case zfgqq(c) => "脂肪肝前期"
      case zfg(c) => if (c.contains("度") || c.contains("非均匀性")) "脂肪肝" else ""
      case yg(c) => "隐睾"
      case zfl(c) => "脂肪瘤"
      case erdjz(c) => "外耳道疖肿"
      case bltjx(c) => "眼底出血"
      case sezcky(c) => "十二指肠球部溃疡"
      case jxnz(c) => "腱鞘囊肿"
      case fwz(c) => "飞蚊症"
      case jsjmqz(c) => "精索静脉曲张"
      case sjxel(c) => "神经性耳聋"
      case gwjy(c) => "睾丸鞘膜积液"
      case gzcw(c) => "脊柱侧弯"
      case gmck(c) => "鼓膜穿孔"
      case gmsz(c) => "结膜炎"
      case rs(c) => "弱视"
      case sy(c) => "沙眼"
      case gjnsnz(c) => "宫颈纳氏囊肿"
      case gjml(c) => "宫颈糜烂"
      case dnqs(c) => "耵聍栓塞"
      case qtdxnz(c) => "前庭大腺囊肿"
      case bj(c) => "包茎"
      case xzjmqz(c) => "下肢静脉曲张"
      case bzgp(c) => "鼻中隔偏曲"
      case bdf(c) => "白癜风"
      case bnz(c) => "白内障"
      case qlx(c) => "前列腺增生"
      case ydy(c) => "阴道炎"
      case pfgr(c) => "皮肤真菌感染"
      case gzjb(c) => "痔疮"
      case fbx(c) => "腹壁疝"
      case sgnbq(c) => "肾功能不全"
      case slqj(c) => "视力欠佳"
      case lxxyzs(c) => "乳腺小叶增生"
      case yjs(c) => "甲状腺结节"
      case mxyy(c) => "慢性咽炎"
      case myzk(c) => "脉压增宽"
      case bdjjd(c) => "白带清洁度偏高"
      case qlxgh(c) => "前列腺钙化灶"
      case dxxdkh(c) => "窦性心动过缓"
      case lbxbbfl(c) => "淋巴细胞百分率偏高"
      case dxlbq(c) => "窦性心律不齐"
      case gljs(c) => "骨量减少"
      case zxlbfl(c) => "中性粒细胞百分率降低"
      case _ => ""
    }
  }

  def objToJson(obj: util.HashMap[String, String]): String = {
    s"""{${jsonField("report_content_id", obj)},${jsonField("health_report_id", obj)},"report_content":${JavaUtils.replaceLineBreak(JavaUtils.replaceLineBreak(obj.get("report_content")))},${jsonField("create_time", obj)},${jsonField("last_update_time", obj)}}"""
  }

  /**
  乙肝表面抗体   乙肝核心抗体   乙肝表面抗原   乙肝e抗原   乙肝e抗体
    -----------------------------------------------------------------------------------
    感染乙肝后已经康复          阳性          阳性
    乙肝恢复期                阳性          阳性                                 阳性
    乙肝小三阳                             阳性         阳性                     阳性
    乙肝大三阳                             阳性         阳性           阳性
    急性乙肝早期                                        阳性           阳性

    HBcAg隐性携带者/
    HBcAg隐性窗口期/                       阳性
    乙肝病毒既往感染史

    急性乙肝感染潜伏期                                    阳性
    后期/乙肝病毒携带者
    */
  def getDiseases(labels: Iterable[String]): String = {
    val otherDiseases = new ArrayBuffer[String]()
    val hepatitisBbuffer = new StringBuffer()
    labels.foreach {
      label =>
        if (label == "乙肝表面抗原" || label == "乙肝核心抗体" || label == "乙肝表面抗体" || label == "乙肝e抗原" || label == "乙肝e抗体")
          hepatitisBbuffer.append(label)
        else if (label != "")
          otherDiseases.+=(label)
    }
    val hepatitisB = hepatitisBbuffer.toString
    if (hepatitisB.contains("乙肝核心抗体") && hepatitisB.contains("乙肝表面抗体")) {
      if (hepatitisB.contains("乙肝e抗体"))
        otherDiseases += "乙肝恢复期"
      else
        otherDiseases += "感染乙肝后已经康复"
    } else if (hepatitisB.contains("乙肝核心抗体") && hepatitisB.contains("乙肝表面抗原")) {
      if (hepatitisB.contains("乙肝e抗原"))
        otherDiseases += "乙肝大三阳"
      else if (hepatitisB.contains("乙肝e抗体"))
        otherDiseases += "乙肝小三阳"
    } else if (hepatitisB.contains("乙肝表面抗原")) {
      if (hepatitisB.contains("乙肝e抗原"))
        otherDiseases += "急性乙肝早期"
      else
        otherDiseases += "急性乙肝感染潜伏期后期/乙肝病毒携带者"
    } else if (hepatitisB.contains("乙肝核心抗体")) {
      otherDiseases += "HBcAg隐性携带者/HBcAg隐性窗口期/乙肝病毒既往感染史"
    }
    otherDiseases.mkString(",")
  }

  private def jsonField(name: String, obj: util.HashMap[String, String]): String = {
    s""""$name":"${obj.get(name)}""""
  }

  def removeBlank(a: Any): String = if (a == null) "" else a.toString

  def rowToHiveFields(r: Row): String = {
    val str = new StringBuilder()
    for (i <- 0 until r.length) {
      str.append(s"${removeBlank(r(i))}${HiveCatalog.delimiter}")
    }
    str.toString()
  }

  def arrayToHiveFields(array: Array[String]): String = {
    val str = new StringBuilder()
    array.foreach(x => str.append(s"${x}${HiveCatalog.delimiter}"))
    str.toString()
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


  def summaryToHive(jsonDF: DataFrame, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val vals = new util.ArrayList[String](1)
    vals.add(JavaUtils.getStrDate("yyyy-MM-dd"))
    val hiveCatalog = new HiveCatalog("report_summary", Array("health_report_id", "summary"), vals)
    val result = jsonDF.map(ParseReport.rowToHiveFields(_))
    logger.info("将summary保存到Hive中")
    hiveCatalog.save(result.collectAsList(), 2)
  }

  def indexToHive(indexDF: DataFrame, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val vals = new util.ArrayList[String](1)
    vals.add(JavaUtils.getStrDate("yyyy-MM-dd"))
    val hiveCatalog = new HiveCatalog("report_index",
      Array("health_report_id", "check_index_name", "check_user_name", "check_index_name",
        "result_value", "unit", "text_ref", "result_flag_id", "can_explain"), vals)
    val result: Dataset[String] = indexDF.map(ParseReport.rowToHiveFields(_))
    logger.info("将index保存到Hive")
    hiveCatalog.save(result.collectAsList(), 2)
  }

  def suggestsToHive(jsonDF: DataFrame, sqlContext: SQLContext) = {
    val vals = new util.ArrayList[String](1)
    vals.add(JavaUtils.getStrDate("yyyy-MM-dd"))
    val hiveCatalog = new HiveCatalog("report_suggest",
      Array("health_report_id", "summary_name", "summary_medical_explanation", "summary_reason_result",
        "summary_advice", "summary_description", "review_advice", "result", "fw"), vals)
    val df = parseSuggestsProcess(jsonDF, sqlContext)
    logger.info("将suggests保存到Hive")
    hiveCatalog.save(df.collectAsList(), 2)
  }

  def getLabesRDD(indexDF: DataFrame, summaryDF: DataFrame, userDF: DataFrame): RDD[(String, Map[String, String])] = {
    val indexLabels = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), ParseReport.genIndexLabel(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2)))))
    val summaryLabels = summaryDF.select("healthReportId", "summary").rdd
      .map(r => (ScalaUtils.toString(r(0)), ParseReport.genSummaryLabel(ScalaUtils.toString(r(1)))))
    val labelRDD = indexLabels.union(summaryLabels).distinct().groupByKey().map(x => (x._1, ParseReport.getDiseases(x._2)))
    val height = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), ParseReport.genHeight(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    val weight = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), ParseReport.genWeight(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    val waistline = indexDF.select("healthReportId", "checkIndexName", "resultValue").rdd
      .map(r => (ScalaUtils.toString(r(0)), ParseReport.genWaistline(ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2))))).filter(_._2 != "")
    //userRDD.rdd: ("healthReportId","userId","idCardNoMd5", "birthday", "sex", "checkUnitCode", "checkUnitName", "checkDate", "lastUpdateTime")
    val userRDD = userDF.rdd.map(r => (ScalaUtils.toString(r(0)), (ScalaUtils.toString(r(1)), ScalaUtils.toString(r(2)), ScalaUtils.toString(r(3)), ScalaUtils.toString(r(4)), ScalaUtils.toString(r(5)), ScalaUtils.toString(r(6)), ScalaUtils.toString(r(7)), ScalaUtils.toString(r(8)))))
    //(2515218,(CompactBuffer((CompactBuffer(类风湿性关节炎,阴道炎,超重),CompactBuffer(150-170),CompactBuffer(65-90),CompactBuffer())),CompactBuffer((761b9c2e-81f6-4f79-850a-bd112988823d,1163ed15c5f3c149fb1167bc018636d1,2017-01-01,女,美年大健康太原长风分院,美年大健康太原长风分院,2017-01-01 11:11:11,2017-12-12))))
    labelRDD.cogroup(height, weight, waistline).cogroup(userRDD)
      .map {
        x =>
          val healthReportId = x._1
          val label = if (x._2._1.isEmpty || x._2._1.head._1.isEmpty) "" else x._2._1.head._1.head
          val height = if (x._2._1.isEmpty || x._2._1.head._2.isEmpty) "" else x._2._1.head._2.head
          val weight = if (x._2._1.isEmpty || x._2._1.head._3.isEmpty) "" else x._2._1.head._3.head
          val waistline = if (x._2._1.isEmpty || x._2._1.head._4.isEmpty) "" else x._2._1.head._4.head
          val userId = if (x._2._2.isEmpty) "" else x._2._2.head._1
          val idCardNoMd5 = if (x._2._2.isEmpty) "" else x._2._2.head._2
          val birthday = if (x._2._2.isEmpty) "" else x._2._2.head._3
          val sex = if (x._2._2.isEmpty) "" else x._2._2.head._4
          val checkUnitCode = if (x._2._2.isEmpty) "" else x._2._2.head._5
          val checkUnitName = if (x._2._2.isEmpty) "" else x._2._2.head._6
          val checkDate = if (x._2._2.isEmpty) "" else x._2._2.head._7
          val lastUpdateTime = if (x._2._2.isEmpty) "" else x._2._2.head._8
          (healthReportId, Map("lastUpdateTime" -> lastUpdateTime,
            "label" -> label,
            "height" -> height,
            "weight" -> weight,
            "waistline" -> waistline,
            "userId" -> userId,
            "idCardNoMd5" -> idCardNoMd5,
            "birthday" -> birthday,
            "sex" -> sex,
            "checkUnitCode" -> checkUnitCode,
            "checkUnitName" -> checkUnitName,
            "checkDate" -> checkDate,
            "lastUpdateTime" -> lastUpdateTime,
            "labelCreateTime" -> JavaUtils.getStrDate))
      }
  }

  def labelsToES(labelRDD: RDD[(String, Map[String, String])]) = {
    logger.info("将解析完的报告存入ES中")
    EsSpark.saveToEsWithMeta(labelRDD, s"${Props.get("es.resource")}")

  }

  def labelsToHive(labelRDD: RDD[(String, Map[String, String])]) = {
    logger.info("将labels保存到Hive中")
    val vals = new util.ArrayList[String](1)
    vals.add(JavaUtils.getStrDate("yyyy-MM-dd"))
    val hiveCatalog = new HiveCatalog("report_label",
      Array("health_report_id", "label", "height", "weight", "waistline",
        "user_id", "id_card_no_md5", "birthday", "sex", "check_unit_code",
        "check_unit_name", "check_date", "last_update_time"
      ), vals)
    val labelArray = labelRDD.map { x =>
      val sb = new StringBuffer()
      sb.append(x._1).append(HiveCatalog.delimiter)
        .append(x._2.get("label").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("height").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("weight").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("waistline").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("userId").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("idCardNoMd5").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("birthday").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("sex").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("checkUnitCode").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("checkUnitName").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("checkDate").getOrElse("")).append(HiveCatalog.delimiter)
        .append(x._2.get("lastUpdateTime").getOrElse("")).append(HiveCatalog.delimiter).toString
    }.collect()
    hiveCatalog.save(labelArray, 2)
  }

  def saveToEsHive(jsonDF: DataFrame, sqlContext: SQLContext) = {
    val indexDF = parseIndexProcess(jsonDF, sqlContext).persist()
    val summaryDF = parseSummaryProcess(jsonDF, sqlContext).persist()
    val userDF = parseUserProcess(jsonDF, sqlContext)
    val labelRDD = getLabesRDD(indexDF, summaryDF, userDF).persist()

    labelsToES(labelRDD)

    labelsToHive(labelRDD)
    indexToHive(indexDF, sqlContext)
    summaryToHive(summaryDF, sqlContext)
    suggestsToHive(jsonDF, sqlContext)

    labelRDD.unpersist()
    indexDF.unpersist()
    summaryDF.unpersist()
  }

  private def parseSuggestsProcess(jsonDF: DataFrame, sqlContext: SQLContext): Dataset[String] = {
    import sqlContext.implicits._
    jsonDF.withColumn("suggest", explode($"reportContent.generalSummarys"))
      .select("healthReportId", "suggest.summaryName", "suggest.summaryMedicalExplanation", "suggest.summaryReasonResult",
        "suggest.summaryAdvice", "suggest.summaryDescription", "suggest.reviewAdvice",
        "suggest.result", "suggest.fw").map(ParseReport.rowToHiveFields(_))
  }
}
