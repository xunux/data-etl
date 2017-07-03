package com.haozhuo.bigdata.dataetl.streamsyn

import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.haozhuo.bigdata.dataetl.Props
import com.haozhuo.bigdata.dataetl.bean.{Article, DBJson, Report, User}
import com.haozhuo.bigdata.dataetl.streamsyn.es.ArticleES
import com.haozhuo.bigdata.dataetl.streamsyn.hive.{UserCatalog, ReportCatalog}
import com.haozhuo.bigdata.dataetl.streamsyn.mysql.{ArticleDAO, ReportDAO, UserDAO}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.slf4j.LoggerFactory

/**
 * Created by LingXin on 6/30/17.
 */
object StreamSyn {
  val userTopic = Props.get("kafka.topic.name.user")
  val reportTopic = Props.get("kafka.topic.name.report")
  val articleTopic = Props.get("kafka.topic.name.article")
}

class StreamSyn(ssc: StreamingContext) extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())

  def processUser(mapperBV: Broadcast[ObjectMapper], userTypeBV: Broadcast[JavaType], rdd: RDD[(String, String)]): Unit = {
    try {
      rdd.filter(_._1 == StreamSyn.userTopic)
        .map(x => mapperBV.value.readValue(x._2, userTypeBV.value).asInstanceOf[DBJson[User]])
        .map(x => x.getObj).foreachPartition {
        itr =>
          val array = itr.toArray
          UserCatalog.insert(array)
          UserDAO.insertOrUpdate(array)
      }
    } catch {
      case e: Exception =>
        logger.error("user出错了", e)
    }
  }

  def processReport(mapperBV: Broadcast[ObjectMapper], reportTypeBV: Broadcast[JavaType], rdd: RDD[(String, String)]): Unit = {
    try {
      rdd.filter(_._1 == StreamSyn.reportTopic)
        .map(x => mapperBV.value.readValue(x._2, reportTypeBV.value).asInstanceOf[DBJson[Report]])
        .filter(x => "INSERT".equalsIgnoreCase(x.getEventType))
        .map(x => x.getObj).foreachPartition {
        itr =>
          val array = itr.toArray
          ReportDAO.insert(array)
          ReportCatalog.insert(array)
      }
    } catch {
      case e: Exception =>
        logger.error("report出错了", e)
    }
  }

  def processArticle(mapperBV: Broadcast[ObjectMapper], informationIdTypeBV: Broadcast[JavaType], rdd: RDD[(String, String)]): Unit = {
    try {
      val articlesArray = rdd.filter(_._1 == StreamSyn.articleTopic)
        .map(x => mapperBV.value.readValue(x._2, informationIdTypeBV.value).asInstanceOf[DBJson[Article]])
        .filter(x => "DELETE".equalsIgnoreCase(x.getEventType))
        .map(x => Long.box(x.getObj.getInformation_id))
        .collect()
      if (articlesArray.length > 0) {
        logger.info("需要处理{}篇文章", articlesArray.length)
        ArticleDAO.delete(articlesArray)
        ArticleES.delete(articlesArray)
      } else {
        logger.info("没有Article需要处理")
      }
    } catch {
      case e: Exception => logger.error("processArticle", e)
    }
  }

  def run(): Unit = {

    val partition = Props.get("spark.stream.partitions").toInt
    val sc = ssc.sparkContext
    val mapper = new ObjectMapper()
    val reportType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[Report])
    val userType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[User])
    //用于删除文章，只传一个information_id.所以类型Long即可
    val articleType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[Article])
    val mapperBV = sc.broadcast(mapper)
    val reportTypeBV = sc.broadcast(reportType)
    val userTypeBV = sc.broadcast(userType)
    val articleTypeBV = sc.broadcast(articleType)
    val topics = Array(StreamSyn.userTopic, StreamSyn.reportTopic, StreamSyn.articleTopic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Props.get("kafka.consumer.group.id"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      .map(x => (x.topic(), x.value())).repartition(1)
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {
            logger.info("no records")
          } else {
            processUser(mapperBV, userTypeBV, rdd)
            processReport(mapperBV, reportTypeBV, rdd)
            processArticle(mapperBV, articleTypeBV, rdd)
          }
      }
  }
}
