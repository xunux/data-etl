package com.haozhuo.bigdata.dataetl.articlefilter.spark

import java.util

import com.alibaba.dubbo.config.{ReferenceConfig, RegistryConfig, ApplicationConfig}

import com.fasterxml.jackson.databind.ObjectMapper
import com.haozhuo.bigdata.dataetl.bean.Article
import com.haozhuo.bigdata.dataetl.spark.SparkUtils
import com.haozhuo.info.dubbo.InfoModuleService
import com.haozhuo.info.entity.vo.param.InformationBigDataParam
import org.apache.spark.rdd.RDD
import com.haozhuo.bigdata.dataetl.articlefilter.simhash.SimHash
import com.haozhuo.bigdata.dataetl.{ScalaUtils, JavaUtils, Props}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

object ArticleFilterStream extends Serializable {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ArticleFilterStream])
  val ARTICLE_ID = "information_id"
  val ARTICLE_TABLE = "article"
  val FINGER_PRINT = "fingerprint"
  val FINGER_PRINT_TEMP_VIEW = "fingerprint_view"

  def getArticle(json: String): Article = {
    var article: Article = null
    try {
      article = new ObjectMapper().readValue(json, classOf[Article])
    } catch {
      case e: Exception =>
        logger.error("获取文章出错了", e)
    }
    article
  }

  def wrapArticles(infoModuleService: InfoModuleService, articles: Array[Article]): Array[Article] = {
    val dataParamList: util.List[InformationBigDataParam] = new util.ArrayList[InformationBigDataParam]()
    articles.foreach {
      article =>
        val dataParam = new InformationBigDataParam()
        dataParam.setFingerprint(article.getFingerprint)
        dataParam.setUrlType(new java.lang.Byte("0"))
        dataParam.setTitle(article.getTitle)
        dataParam.setImage(article.getImage_thumbnail)
        dataParam.setImages(article.getImage_list)
        dataParam.setContent(article.getAbstracts)
        dataParam.setStartTime(JavaUtils.strToDate(article.getCreate_time, "yyyy-MM-dd HH:mm:ss"))
        dataParam.setDetail(article.getHtmls)
        dataParam.setSource(article.getSource)
        dataParamList.add(dataParam)
    }
    logger.info("开始调用dubbo进行包装")
    val infoIds = infoModuleService.addInformationByBigData(dataParamList)
    logger.info("包装结束")
    articles.map {
      article =>
        for (entry <- infoIds.entrySet() if entry.getKey == article.getFingerprint) {
          article.setInformation_id(entry.getValue.toLong)
        }
        article
    }
  }

  def notWrapArticles(articles: Array[Article]): Array[Article] = {
    articles.map {
      article =>
        article.setInformation_id(article.getFingerprint)
        article
    }
  }
}


class ArticleFilterStream(ssc: StreamingContext, session: SparkSession) extends Serializable {

  val logger = LoggerFactory.getLogger(getClass())

  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Props.get("kafka.consumer.group.id"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )

  val topics = Array(Props.get("kafka.topic.name.articlefilter"))

  def loadAllFingerPrints() = {
    SparkUtils.loadFromMysql(s"${ArticleFilterStream.ARTICLE_ID},${ArticleFilterStream.FINGER_PRINT}", ArticleFilterStream.ARTICLE_TABLE, ArticleFilterStream.ARTICLE_ID, Props.get("spark.stream.partitions").toInt)
      .select(ArticleFilterStream.FINGER_PRINT)
      .createOrReplaceTempView(ArticleFilterStream.FINGER_PRINT_TEMP_VIEW)
  }

  def instanceInfoModuleService(): InfoModuleService = {
    val application: ApplicationConfig = new ApplicationConfig()
    application.setName(Props.get("dubbo.application.name"))

    // 连接注册中心配置
    val registry: RegistryConfig = new RegistryConfig()
    //registry.setProtocol("zookeeper")
    registry.setAddress(Props.get("dubbo.registry.address"))
    registry.setTimeout(Props.get("dubbo.timeout").toInt)

    // 注意：ReferenceConfig为重对象，内部封装了与注册中心的连接，以及与服务提供方的连接
    // 引用远程服务
    val reference: ReferenceConfig[InfoModuleService] = new ReferenceConfig[InfoModuleService]() // 此实例很重，封装了与注册中心的连接以及与提供者的连接，请自行缓存，否则可能造成内存和连接泄漏
    reference.setApplication(application)
    reference.setRegistry(registry) // 多个注册中心可以用setRegistries()
    reference.setInterface(classOf[InfoModuleService])

    // reference.setVersion(Props.get("dubbo.reference.wrapservce.version")) //这个一定要与provider的version一致
    logger.info("dubbo准备获取InfoModuleService")
    reference.get(); // 注意：此代理对象内部封装了所有通讯细节，对象较重，请缓存复用
  }

  def run() = {
    logger.info("run()")
    val isWrap = Props.get("iswrap").toBoolean
    var infoModuleService: InfoModuleService = null
    if (isWrap) infoModuleService = instanceInfoModuleService()
    logger.info("dubbo获取InfoModuleService")
    loadAllFingerPrints()
    val spark = SparkUtils.getOrCreateSparkSession()

    val nearDuplicateDistance = Props.get("simhash.nearDuplicateDistance").toInt
    val partitionNum = Props.get("spark.stream.partitions").toInt

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      .map(x => ArticleFilterStream.getArticle(x.value()))
      .filter(x => x != null)
      .map(x => (SimHash.simhash64(x.content), x))
      .reduceByKey((x, y) => x)
      .map { x => x._2.setFingerprint(x._1); x._2 }
      .repartition(partitionNum)
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {
            logger.info("no records")
          } else {
            logger.info("has records")
            try {
              import spark.implicits._
              //对新来的指纹进行去重
              val fingerRDD = rdd.map(_.getFingerprint).persist()
              val fingerCount = fingerRDD.count()
              logger.info("等待去重的文章数:{}", fingerCount)
              val dup1 = fingerRDD.cartesian(fingerRDD)//.filter(x => x._1 != x._2)
                .map(x => (x._1, x._2, SimHash.hammingDistance(x._1, x._2)))
                .filter(x => x._3 < nearDuplicateDistance && x._1 < x._2).map(_._1)
              val fingerprintWithoutDup1 = fingerRDD.subtract(dup1).persist()

              val oldFingerprint = spark.sql(s"select ${ArticleFilterStream.FINGER_PRINT} from ${ArticleFilterStream.FINGER_PRINT_TEMP_VIEW}")
                .map(r => ScalaUtils.toString(r(0)).toLong).persist()
                .rdd
              //对比新的指纹与内存表中的指纹，过滤掉重复的指纹
              val dup2 = fingerprintWithoutDup1.cartesian(oldFingerprint)
                .map(x => (x._1, x._2, SimHash.hammingDistance(x._1, x._2)))
                .filter(_._3 < nearDuplicateDistance).map(_._1)

              //得到新的指纹
              val newFingerprint = fingerprintWithoutDup1.subtract(dup2).persist()

              //将新的指纹加入到内存表中
              oldFingerprint.union(newFingerprint).toDF(ArticleFilterStream.FINGER_PRINT)
                .createOrReplaceTempView(ArticleFilterStream.FINGER_PRINT_TEMP_VIEW)

              val newFingerprintArray = newFingerprint.collect()
              val articlesWaitForWrap = rdd.filter(x => newFingerprintArray.contains(x.getFingerprint)).persist()
              val articlesWaitForWrapArray = articlesWaitForWrap.collect()
              logger.info("去重后的文章数量:{}", articlesWaitForWrapArray.length)
              if (articlesWaitForWrapArray.length > 0) {
                //包装
                val articlesAfterWrap =
                  if (isWrap) {
                    ArticleFilterStream.wrapArticles(infoModuleService, articlesWaitForWrapArray)
                  } else {
                    //不进行包装
                    logger.warn("没有进行包装！！！")
                    ArticleFilterStream.notWrapArticles(articlesWaitForWrapArray): Array[Article]
                  }

                val newArticles: RDD[Article] = spark.sparkContext.parallelize[Article](articlesAfterWrap).persist()

                //将新的文章加入到mysql中的表中
                logger.info("存到mysql中的表中...")
                SparkUtils.saveToMySql(newArticles.toDF(), ArticleFilterStream.ARTICLE_TABLE)

                //存ES
                logger.info("存到ES中...")
                val esResult = newArticles.map(x => (x.getInformation_id, Map(
                  "abstracts" -> x.getAbstracts,
                  "content" -> x.getContent,
                  "create_time" -> x.getCreate_time,
                  "htmls" -> x.getHtmls,
                  "title" -> x.getTitle
                )))
                EsSpark.saveToEsWithMeta(esResult, s"${Props.get("es.resource")}")
              }
              logger.info("存储完毕，释放RDD")
              //释放缓存
              articlesWaitForWrap.unpersist()
              fingerprintWithoutDup1.unpersist()
              newFingerprint.unpersist()
              oldFingerprint.unpersist()
              fingerRDD.unpersist()
              logger.info("本批次文章处理完毕")
            } catch {
              case e: Exception =>
                logger.error("文章去重出错了", e)
            }
          }
      }
  }
}
