package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import com.haozhuo.bigdata.dataetl.Props
import org.apache.spark.sql.functions.upper
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.slf4j.LoggerFactory

class StreamParseJson(ssc: StreamingContext) extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())
  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Props.get("kafka.consumer.group.id"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )

  val topics = Array(Props.get("kafka.topic.name"))

  def run(): Unit = {
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    stream.map(_.value())
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {
            logger.info("no records")
          } else {
            logger.info("has records")
            try {
              val spark = SparkSession.builder.getOrCreate()
              val sqlContext = spark.sqlContext
              import spark.implicits._

              val jsonDF = sqlContext.read.json(rdd).filter(upper($"eventType") === "INSERT" && upper($"table") === "REPORT")
                .select("obj.healthReportId","obj.userId","obj.idCardNoMd5", "obj.birthday", "obj.sex", "obj.checkUnitCode", "obj.checkUnitName","obj.reportContent","obj.checkDate","obj.lastUpdateTime").persist()
              val parseReport = new ParseReport()
              parseReport.saveToEsHive(jsonDF, sqlContext)
              jsonDF.unpersist()
             // parseReport.reportToHive(rdd, sqlContext)
            } catch {
              case e: Exception =>
                logger.error("解析报告出错了",e)
            }
         }
      }
  }
}

