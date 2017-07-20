package com.haozhuo.bigdata.dataetl.spark

import java.io.Serializable
import java.util.Properties
import com.haozhuo.bigdata.dataetl.{ScalaUtils, Props}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkUtils extends Serializable {
  private var conf: SparkConf = _

  val MYSQL_CONNECTION_URL = Props.get("mysql.connection.url")
  val connectionProperties = new Properties()
  connectionProperties.put("user", Props.get("mysql.connection.user"))
  connectionProperties.put("password", Props.get("mysql.connection.password"))

  def getOrCreateSparkSession(loadConf: Boolean = true): SparkSession = {
    if (loadConf) {
      SparkSession.builder.config(getConf).getOrCreate()
    } else {
      SparkSession.builder.getOrCreate()
    }
  }

  def createStream(interval: Int = Props.get("spark.stream.interval").toString.toInt): StreamingContext = {
    new StreamingContext(getOrCreateSparkSession().sparkContext, Seconds(interval))
  }

  def getConf(): SparkConf = {
    if (conf == null) {
      val conf = new SparkConf()
      conf.set("spark.app.name", conf.get("spark.app.name", "spark app"))
      val sparkMaster = conf.get("spark.master", "local[4]")
      conf.set("spark.master", sparkMaster)
      if (sparkMaster == "local[4]") {
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", Props.get("es.nodes"))
        conf.set("es.port", Props.get("es.port"))
        conf.set("spark.streaming.kafka.maxRatePerPartition", "1")
      }
      this.conf = conf
    }
    this.conf
  }

  /*  def getOffsetRanges(topic: String, groupId: String): Array[OffsetRange] = {
      val spark = getOrCreateSparkSession(false)
      import spark.implicits._
      spark.read.jdbc(MYSQL_CONNECTION_URL, s" (select * from kafka_topic_offset where topic='${topic}' and group_id = '${groupId}') as kafka_topic ", connectionProperties)
        .rdd.map(r => OffsetRange(ScalaUtils.toString(r(0)), ScalaUtils.toString(r(2)).toInt, ScalaUtils.toString(r(3)).toLong, Long.MaxValue))
        .collect()
    }*/

  private def getOffsetRanges(topicAndGroupIdArray: Array[(String, String)]): Array[OffsetRange] = {
    val spark = getOrCreateSparkSession(false)
    import spark.implicits._
    topicAndGroupIdArray.flatMap {
      topicAndPartition =>
        spark.read.jdbc(MYSQL_CONNECTION_URL, s" (select * from kafka_topic_offset where topic='${topicAndPartition._1}' and group_id = '${topicAndPartition._2}') as kafka_topic ", connectionProperties)
          .rdd.map(r => OffsetRange(ScalaUtils.toString(r(0)), ScalaUtils.toString(r(2)).toInt, ScalaUtils.toString(r(3)).toLong, Long.MaxValue))
          .collect()
    }
  }

  /*  def getOffsetMap(topic: String, groupId: String): collection.Map[TopicPartition, Long] = {
      getOffsetRanges(topic, groupId).map {
        offsetRange => new TopicPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.fromOffset
      }.toMap
    }*/

  def getOffsetMap(topicAndGroupIdArray: Array[(String, String)]): collection.Map[TopicPartition, Long] = {
    getOffsetRanges(topicAndGroupIdArray).map {
      offsetRange => new TopicPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.fromOffset
    }.toMap
  }

  def getOffsetMap(topic: String, groupId: String): collection.Map[TopicPartition, Long] = {
    getOffsetMap(Array((topic, groupId)))
  }

  def commitKafkaOffset(offsets: RDD[KafkaTopicOffset]) = {
    val spark = getOrCreateSparkSession(false)
    import spark.implicits._
    val array = offsets.map(x => ((x.topic, x.groupId, x.partition), x.offset))
      .reduceByKey((x, y) => if (x > y) x else y).map(x => KafkaTopicOffset(x._1._1, x._1._2, x._1._3, x._2+1)).collect()
    new KafkaTopicOffsetDAO().insertOrUpdate(array)
  }

/*  def loadFromMysql(columns: String, table: String): DataFrame = {
    val spark = getOrCreateSparkSession(false)
    spark.read.jdbc(MYSQL_CONNECTION_URL, s" (select $columns from $table) as tb", connectionProperties)
  }*/
  def loadFromMysql(columns: String, table: String, condition: String = "1=1"): DataFrame = {
    val spark = getOrCreateSparkSession(false)
    spark.read.jdbc(MYSQL_CONNECTION_URL, s" (select $columns from $table where $condition ) as tb", connectionProperties)
  }

  def saveToMySql(sql: String, table: String): Unit = {
    getOrCreateSparkSession().sql(sql).write.mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, table, connectionProperties)
  }

  def saveToMySql(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, table, connectionProperties)
  }

  def saveToMySql(df: DataFrame, table: String, saveMode: SaveMode): Unit = {
    df.write.mode(saveMode).jdbc(MYSQL_CONNECTION_URL, table, connectionProperties)
  }

  def clearAllRDDs(): Unit = {
    val persistentRDDs = getOrCreateSparkSession(loadConf = false).sparkContext.getPersistentRDDs
    for ((id, rdd) <- persistentRDDs) {
      rdd.unpersist()
    }
  }

  def clearAllRDDs(sc: SparkContext): Unit = {
    val persistentRDDs = sc.getPersistentRDDs
    for ((id, rdd) <- persistentRDDs) {
      rdd.unpersist()
    }
  }
}
