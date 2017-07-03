package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import java.util
import com.haozhuo.bigdata.dataetl.Props
import com.haozhuo.bigdata.dataetl.labelgen.kafka.HashMapSerde
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._


class StreamParseHashMap(ssc: StreamingContext) extends Serializable {
  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[HashMapSerde],
      "group.id" -> "test_consumer_group_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )

  val topics = Array(Props.get("kafka.topic.name"))

  def run(): Unit = {
    val stream = KafkaUtils.createDirectStream[String, util.HashMap[String,String]](ssc, PreferConsistent, Subscribe[String, util.HashMap[String,String]](topics, kafkaParams))
    stream.filter(x => x.value().get("eventType") == "INSERT")
      .map(x => (x.value, ParseReport.objToJson(x.value())))
      .repartition(1)
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {

          } else {
            val sqlContext = SparkSession.builder.getOrCreate().sqlContext
            val objToJsonRDD = rdd.map(_._2)
            val jsonDF = sqlContext.read.json(objToJsonRDD).persist()
            val pas = new ParseReport()
       /*     pas.indexToHive(jsonDF, sqlContext)
            pas.suggestsToHive(jsonDF, sqlContext)
            pas.summaryToHive(jsonDF, sqlContext)*/
            //pas.parseReportContentForStream(rdd.map(_._1), sqlContext)
            jsonDF.unpersist()
          }
      }
  }
}

