package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.haozhuo.bigdata.dataetl.{JavaUtils, Props}
import com.haozhuo.bigdata.dataetl.bean.{Report, User, DBJson}
import com.haozhuo.bigdata.dataetl.spark.{KafkaTopicOffset, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.upper
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.slf4j.LoggerFactory

object StreamParseJson extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())

  val groupId = Props.get("kafka.consumer.group.id")
  val reportTopic = Props.get("kafka.topic.name.report")
  val userTopic = Props.get("kafka.topic.name.user")

  def processReport(spark:SparkSession,rdd:RDD[(KafkaTopicOffset,String)]) = {
    import spark.implicits._

    val reportRDD = rdd.filter(_._1.topic==StreamParseJson.reportTopic).map(_._2).persist()
    val count = reportRDD.count()
    if(count>0){
      val parseReport = new ParseReport()
      val sqlContext = spark.sqlContext
      logger.info("有{}个Report需要处理",count)
      val jsonDF = sqlContext.read.json(reportRDD).filter(upper($"eventType") === "INSERT" && upper($"table") === "REPORT")
        .select("obj.healthReportId", "obj.userId", "obj.idCardNoMd5", "obj.birthday", "obj.sex", "obj.checkUnitCode", "obj.checkUnitName", "obj.reportContent", "obj.checkDate", "obj.lastUpdateTime").persist()
      parseReport.saveToEsHBase(jsonDF, sqlContext)
      parseReport.saveReport(reportRDD)
    }else{
      logger.info("没有Report需要处理")
    }
  }

  def processUser(spark:SparkSession,rdd: RDD[(KafkaTopicOffset, String)]): Unit = {
    try {
      val userRDD  = rdd.filter(_._1.topic == StreamParseJson.userTopic).persist()
      val count = userRDD.count()
      if(count > 0){
        logger.info("有{}个User需要处理",count)
        val mapper = new ObjectMapper()
        val userType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[User])
        val parseUser = new ParseUser()
        val array = userRDD.map(x => mapper.readValue(x._2, userType).asInstanceOf[DBJson[User]]).map(x => x.getObj).collect()
        parseUser.mysqlInsertOrUpdate(array)
        parseUser.hbaseInsertOrUpdate(array)
      } else {
        logger.info("没有User需要处理")
      }

    } catch {
      case e: Exception => logger.error("processUser出错了", e)
    }
  }
}

class StreamParseJson(ssc: StreamingContext) extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())
  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> StreamParseJson.groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )


  def run(): Unit = {
    val offsets = SparkUtils.getOffsetMap(Array((StreamParseJson.reportTopic, StreamParseJson.groupId),(StreamParseJson.userTopic, StreamParseJson.groupId)))
    logger.info("offsets:{}", offsets)

    val sc = ssc.sparkContext
    val spark = SparkSession.builder.getOrCreate()

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(StreamParseJson.reportTopic,StreamParseJson.userTopic), kafkaParams,offsets))
      .map(x => (KafkaTopicOffset(x.topic(), StreamParseJson.groupId,x.partition(), x.offset()), x.value())).repartition(1)
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {
            logger.info("no records")
          } else {
            logger.info("has records")
            try {
              StreamParseJson.processReport(spark,rdd)
              StreamParseJson.processUser(spark,rdd)
              SparkUtils.commitKafkaOffset(rdd.map(_._1))
              SparkUtils.clearAllRDDs(sc)
            } catch {
              case e: Exception =>
                logger.error("解析报告出错了", e)
            }
          }
          logger.info("该批次完成")
      }
  }
}

