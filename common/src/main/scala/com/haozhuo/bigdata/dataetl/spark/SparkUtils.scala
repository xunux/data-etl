package com.haozhuo.bigdata.dataetl.spark

import java.io.Serializable
import java.util.Properties

import com.haozhuo.bigdata.dataetl.Props
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkUtils extends Serializable {
  private var conf: SparkConf = _

  val MYSQL_CONNECTION_URL = Props.get("mysql.connection.url")
  val connectionProperties = new Properties()
  connectionProperties.put("user", Props.get("mysql.connection.user"))
  connectionProperties.put("password", Props.get("mysql.connection.password"))

  def getOrCreateSparkSession(): SparkSession = {
    SparkSession.builder.config(getConf).getOrCreate()
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
      }
      this.conf = conf
    }
    this.conf
  }

  def loadFromMysql(columns: String, table: String, spiltColumn: String, partitionNum: Int = Props.get("spark.sql.partitions", "5").toInt): DataFrame = {
    val spark = getOrCreateSparkSession
    import spark.implicits._
    val max = spark.read.jdbc(
      MYSQL_CONNECTION_URL,
      s"(select max($spiltColumn) from $table) as column_id",
      connectionProperties
    ).map(r => if (r(0) == null) 1 else r(0).toString.toLong).collect()(0)
    val min = spark.read.jdbc(
      MYSQL_CONNECTION_URL,
      s"(select min($spiltColumn) from $table) as column_id",
      connectionProperties
    ).map(r => if (r(0) == null) 1 else r(0).toString.toLong).collect()(0)
    spark.read.jdbc(MYSQL_CONNECTION_URL,
      s" (select $columns from $table) as tb", spiltColumn, min, max, partitionNum, connectionProperties)
  }

  def saveToMySql(sql: String, table: String): Unit = {
    getOrCreateSparkSession.sql(sql).write.mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, table, connectionProperties)
  }

  def saveToMySql(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, table, connectionProperties)
  }
}
