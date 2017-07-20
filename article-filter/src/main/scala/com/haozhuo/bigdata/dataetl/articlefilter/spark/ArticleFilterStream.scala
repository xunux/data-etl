package com.haozhuo.bigdata.dataetl.articlefilter.spark

import java.sql.{SQLException, PreparedStatement, Connection}
import java.util

import com.alibaba.dubbo.config.{ReferenceConfig, RegistryConfig, ApplicationConfig}

import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.haozhuo.bigdata.dataetl.articlefilter.es.ArticleES
import com.haozhuo.bigdata.dataetl.bean.{DBJson, Article}
import com.haozhuo.bigdata.dataetl.hbase._
import com.haozhuo.bigdata.dataetl.mysql.DataSource
import com.haozhuo.bigdata.dataetl.spark.{KafkaTopicOffset, SparkUtils}
import com.haozhuo.info.dubbo.InfoModuleService
import com.haozhuo.info.entity.vo.param.InformationBigDataParam
import org.apache.spark.broadcast.Broadcast
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
  val topic = Props.get("kafka.topic.name.articlefilter")
  val groupId = Props.get("kafka.consumer.group.id")
  val nearDuplicateDistance = Props.get("simhash.nearDuplicateDistance").toInt
  val isWrap = Props.get("iswrap").toBoolean
  val isSaveToES = Props.get("savetoes").toBoolean

  def mysqlDelete(deleteInfos: Array[(Long,String)]) {
    logger.info("mysqlDelete:{}", deleteInfos.length)
    if (deleteInfos.length == 0) return
    var conn: Connection = null
    var preparedStmt: PreparedStatement = null
    val query: String = "UPDATE article SET is_delete = 1,upd_t = ? WHERE information_id = ?"
    try {
      conn = DataSource.getInstance.getConnection
      preparedStmt = conn.prepareStatement(query)
      conn.setAutoCommit(false)
      var i: Int = 0
      while (i < deleteInfos.length) {
        preparedStmt.setString(1, deleteInfos(i)._2)
        preparedStmt.setLong(2, deleteInfos(i)._1)
        preparedStmt.addBatch
        i = i + 1
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
        } catch {
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

  def mysqlUpdate(articles: Array[Article]) {
    logger.info("mysqlUpdate:" + articles.length)
    if (articles.length == 0) return
    var conn: Connection = null
    var preparedStmt: PreparedStatement = null
    val query: String = "INSERT INTO article (information_id, fingerprint, title, image_list, image_thumbnail, abstracts, content, source, display_url,htmls,create_time,crawler_time,is_delete,comment_count,news_category,data_source,upd_t) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE fingerprint = ?, title=?, image_list=?, image_thumbnail=?, abstracts=?, content=?, source=?, display_url=?,htmls=?,create_time=?,crawler_time=?,is_delete=?,comment_count=?,news_category=?,data_source=?,upd_t=?";

    try {
      conn = DataSource.getInstance.getConnection
      preparedStmt = conn.prepareStatement(query)
      conn.setAutoCommit(false)
      var i: Int = 0
      while (i < articles.length) {
        val article = articles(i)
        preparedStmt.setLong(1, article.getInformation_id)
        preparedStmt.setLong(2, article.getFingerprint)
        preparedStmt.setString(3, article.getTitle)
        preparedStmt.setString(4, article.getImage_list)
        preparedStmt.setString(5, article.getImage_thumbnail)
        preparedStmt.setString(6, article.getAbstracts)
        preparedStmt.setString(7, article.getContent)
        preparedStmt.setString(8, article.getSource)
        preparedStmt.setString(9, article.getDisplay_url)
        preparedStmt.setString(10, article.getHtmls)
        preparedStmt.setString(11, article.getCreate_time)
        preparedStmt.setString(12, article.getCrawler_time)
        preparedStmt.setInt(13, article.getIs_delete)
        preparedStmt.setInt(14, article.getComment_count)
        preparedStmt.setString(15, article.news_category)
        preparedStmt.setString(16, article.getData_source)
        preparedStmt.setString(17, article.getUpd_t)
        preparedStmt.setLong(18, article.getFingerprint)
        preparedStmt.setString(19, article.getTitle)
        preparedStmt.setString(20, article.getImage_list)
        preparedStmt.setString(21, article.getImage_thumbnail)
        preparedStmt.setString(22, article.getAbstracts)
        preparedStmt.setString(23, article.getContent)
        preparedStmt.setString(24, article.getSource)
        preparedStmt.setString(25, article.getDisplay_url)
        preparedStmt.setString(26, article.getHtmls)
        preparedStmt.setString(27, article.getCreate_time)
        preparedStmt.setString(28, article.getCrawler_time)
        preparedStmt.setInt(29, article.getIs_delete)
        preparedStmt.setInt(30, article.getComment_count)
        preparedStmt.setString(31, article.news_category)
        preparedStmt.setString(32, article.getData_source)
        preparedStmt.setString(33, article.getUpd_t)
        preparedStmt.addBatch
        i = i + 1
      }
      preparedStmt.executeBatch
      conn.commit
      conn.setAutoCommit(true)
    } catch {
      case e: Exception => {
        logger.info("Error", e)
      }
    } finally {
      if (preparedStmt != null) {
        try {
          preparedStmt.close
        } catch {
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

  def getDBJSONArticle(mapperBV: Broadcast[ObjectMapper], informationIdTypeBV: Broadcast[JavaType], json: String): DBJson[Article] = {
    var article: DBJson[Article] = null
    try {
      article = mapperBV.value.readValue(json, informationIdTypeBV.value).asInstanceOf[DBJson[Article]]
    } catch {
      case e: Exception =>
        logger.error("获取文章出错了", e)
    }
    article
  }

  def wrapArticles(infoModuleService: InfoModuleService, articles: Array[Article]): Array[Article] = {
    val updateTime = JavaUtils.getStrDate()
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
        dataParam.setDataSource(article.getData_source)
        dataParam.setNewsCategory(article.getNews_category)
        dataParamList.add(dataParam)
    }

    logger.info("开始调用dubbo进行包装")
    val infoIds = infoModuleService.addInformationByBigData(dataParamList)
    logger.info("包装结束,返回的结果：{}",infoIds)

    articles.map {
      article =>
        article.setInformation_id(infoIds.get(article.getFingerprint).toLong)
        article.setUpd_t(updateTime)
        article
    }
  }

  def notWrapArticles(articles: Array[Article]): Array[Article] = {
    val updateTime = JavaUtils.getStrDate()
    articles.map {
      article =>
        //article.setInformation_id(article.getFingerprint)
        article.setUpd_t(updateTime)
        article
    }
  }

  def doWrap(isWrap: Boolean, infoModuleService: InfoModuleService, articlesWaitForWrapArray: Array[Article]) = {
    //包装
    var articlesAfterWrap: Array[Article] = null
    if (isWrap) {
      articlesAfterWrap = ArticleFilterStream.wrapArticles(infoModuleService, articlesWaitForWrapArray): Array[Article]
    } else {
      //不进行包装
      logger.warn("没有进行包装！！！")
      articlesAfterWrap = ArticleFilterStream.notWrapArticles(articlesWaitForWrapArray)
    }
    articlesAfterWrap
  }

  def hbaseUpdate(articles: Array[Article]) = {
    val array = articles.map {
      x =>
        HBaseRow(
          ScalaUtils.toString(x.information_id),
          Array(
            HBaseColumn("fp", ScalaUtils.toString(x.fingerprint)),
            HBaseColumn("title", ScalaUtils.toString(x.title)),
            HBaseColumn("img_l", ScalaUtils.toString(x.image_list)),
            HBaseColumn("img_t", ScalaUtils.toString(x.image_thumbnail)),
            HBaseColumn("abs", ScalaUtils.toString(x.abstracts)),
            HBaseColumn("cont", ScalaUtils.toString(x.content)),
            HBaseColumn("src", ScalaUtils.toString(x.source)),
            HBaseColumn("url", ScalaUtils.toString(x.display_url)),
            HBaseColumn("html", ScalaUtils.toString(x.htmls)),
            HBaseColumn("cra_t", ScalaUtils.toString(x.crawler_time)),
            HBaseColumn("cre_t", ScalaUtils.toString(x.create_time)),
            HBaseColumn("del", ScalaUtils.toString(x.is_delete)),
            HBaseColumn("cmt", ScalaUtils.toString(x.comment_count)),
            HBaseColumn("cat", ScalaUtils.toString(x.news_category)),
            HBaseColumn("data_src", ScalaUtils.toString(x.data_source)),
            HBaseColumn("upd_t", ScalaUtils.toString(x.upd_t))
          ).filter(_.getValue != "")
        )
    }
    logger.info("将数据保存到HBase的DATAETL:ART中")
    new HBaseClient().putInto(new HBaseTable("DATAETL:ART", array))
  }

  def hbaseDelete(delteInfos: Array[(Long,String)]) = {
    val array = delteInfos.map {
      x =>
        HBaseRow(
          ScalaUtils.toString(x._1),
          Array(
            HBaseColumn("del", "1"),
            HBaseColumn("upd_t", ScalaUtils.toString(x._2))
          )
        )
    }
    logger.info("HBase DATAETL:ART DELETE 数据")
    new HBaseClient().putInto(new HBaseTable("DATAETL:ART", array))
  }

  def esUpdate(rdd: RDD[Article]) = {
    //存ES
    logger.info("存到ES中...")
    val esResult = rdd.map(x => (x.getInformation_id, Map(
      "abstracts" -> x.getAbstracts,
      "content" -> x.getContent,
      "htmls" -> x.getHtmls,
      "title" -> x.getTitle,
      "comment_count" -> x.getComment_count,
      "news_category" -> x.getNews_category,
      "create_time" -> x.getCreate_time,
      "crawler_time" -> x.getCrawler_time
    )))

    EsSpark.saveToEsWithMeta(esResult, s"${Props.get("es.resource")}")
  }


  def insertArticles(spark: SparkSession, rdd: RDD[Article], infoModuleService: InfoModuleService) = {
    import spark.implicits._
    val articleRDD: RDD[Article] = genSimHash(rdd).persist()
    val count = articleRDD.count()
    if (count != 0) {
      logger.info(s"爬虫有{}条数据需要处理", count)
      val idFingerRDD = articleRDD.map(x => (x.getInformation_id, x.getFingerprint)).persist()
      //println("本批次中各个文章比较后，认为某篇文章与另一篇是相近的。去掉该(information_id,fingerprint)")
      //本批次中各个文章比较后，认为某篇文章与另一篇是相近的。去掉该(information_id,fingerprint)
      val dupInThisBatch = getIdFingerWithNearHammingDistanceForINSERT(idFingerRDD, idFingerRDD)
        .filter(x => x._1._2 < x._2._2)
        .map(x => if (x._1._1 > x._2._1) (x._2._1, x._2._2) else (x._1._1, x._1._2))

      //本批次内各个文章比较后，不相近的(information_id,fingerprint)
      val uniqueInThisBatch = idFingerRDD.subtract(dupInThisBatch)

      //本批次不相近的文章与内存中的所有文章比对。计算出与内存中的文章相近的本批次的文章
      val idFingersInMemory = getIdFingersInMemory(spark).persist()
      val dupIdFingers = getIdFingerWithNearHammingDistanceForINSERT(uniqueInThisBatch, idFingersInMemory).map(_._1)
      //nearDupIdFingers.collect().foreach(println)

      //println("得到最终的不重复的文章(information_id,fingerprint)")
      //得到最终的不重复的文章(information_id,fingerprint)
      val finalUniqueIdFingers = uniqueInThisBatch.subtract(dupIdFingers).persist()
      val finalUniqueIdFingersArray = finalUniqueIdFingers.collect()

      logger.info("爬虫去重后的文章数:{}", finalUniqueIdFingersArray.length)
      val finalUniqueArticles = articleRDD.filter(x => finalUniqueIdFingersArray.contains((x.getInformation_id, x.getFingerprint))).persist()
      val finalUniqueArticlesArray = finalUniqueArticles.collect()

      if (finalUniqueIdFingersArray.length > 0) {
        //包装
        val articlesAfterWrap: Array[Article] = ArticleFilterStream.doWrap(isWrap, infoModuleService, finalUniqueArticlesArray)

        articlesAfterWrap.map(x => (x.getInformation_id, x.getFingerprint))

        //更新内存表
        unionIdFingerView(spark, articlesAfterWrap.map(x => (x.getInformation_id, x.getFingerprint)), idFingersInMemory.collect())

        val newArticles: RDD[Article] = spark.sparkContext.parallelize[Article](articlesAfterWrap).persist()

        //存ES
        if(ArticleFilterStream.isSaveToES){
          ArticleFilterStream.esUpdate(newArticles)
        }

        //存HBase
        ArticleFilterStream.hbaseUpdate(articlesAfterWrap)

        logger.info("存到mysql中的表中...")
        SparkUtils.saveToMySql(newArticles.toDF(), ArticleFilterStream.ARTICLE_TABLE)
      }
    } else {
      logger.info(s"爬虫无新增数据")
    }
    logger.info("存储完毕，释放RDD")
    SparkUtils.clearAllRDDs(spark.sparkContext)
  }

  def updateArticles(spark: SparkSession, rdd: RDD[Article], infoModuleService: InfoModuleService) = {
    val articleRDD: RDD[Article] = genSimHash(rdd, rmDup = false).persist()
    val count = articleRDD.count()
    if (count != 0) {
      logger.info(s"有{}条数据需UPDATE", count)
      val idFingerRDD = articleRDD.map(x => (x.getInformation_id, x.getFingerprint)).persist()
      //println("本批次中各个文章比较后，认为某篇文章与另一篇是相近的。去掉该(information_id,fingerprint)")
      //本批次中各个文章比较后，认为某篇文章与另一篇是相近的。去掉该(information_id,fingerprint)
      val dupInThisBatch = getIdFingerWithNearHammingDistanceForUPDATE(idFingerRDD, idFingerRDD)
        .filter(x => x._1._1 < x._2._1)
        .map(x => (x._2._1, x._2._2))
      //println("本批次内各个文章比较后，重复的(information_id,fingerprint)")
      //dupInThisBatch.collect().foreach(println)
      //println("本批次内各个文章比较后，不相近的(information_id,fingerprint)")
      //本批次内各个文章比较后，不相近的(information_id,fingerprint)
      val uniqueInThisBatch = idFingerRDD.subtract(dupInThisBatch)
      //uniqueInThisBatch.collect().foreach(println)
      //println("本批次不相近的文章与内存中的所有文章比对。计算出与内存中的文章相近的本批次的文章")
      //本批次不相近的文章与内存中的所有文章比对。计算出与内存中的文章相近的本批次的文章
      val idFingersInMemory = getIdFingersInMemory(spark).persist()
      val dupIdFingers = getIdFingerWithNearHammingDistanceForUPDATE(uniqueInThisBatch, idFingersInMemory).map(_._1)
      //dupIdFingers.collect().foreach(println)

      //println("得到最终的不重复的文章(information_id,fingerprint)")
      //得到最终的不重复的文章(information_id,fingerprint)
      val finalUniqueIdFingers = uniqueInThisBatch.subtract(dupIdFingers).persist()
      val finalUniqueIdFingersArray = finalUniqueIdFingers.collect()
      //finalUniqueIdFingers.collect().foreach(println)

      //println("得到最终的重复的文章(information_id,fingerprint)")
      //得到最终的重复的文章(information_id,fingerprint)
      //val finalDupIdFingers = dupIdFingers.union(dupInThisBatch)
      //finalDupIdFingers.collect().foreach(println)
      val finalUniqueArticles = articleRDD.filter(x => finalUniqueIdFingersArray.contains((x.getInformation_id, x.getFingerprint))).persist()
      val finalUniqueArticlesArray = finalUniqueArticles.collect()
      val finalDupArticles = articleRDD.subtract(finalUniqueArticles)
      val finalDupArticlesArray = finalDupArticles.collect()

      logger.info("编辑(UPDATE)后，使得与其他文章不重复的有{}条", finalUniqueArticlesArray.length)
      if (finalUniqueArticlesArray.length > 0) {
        //更新内存中的(information_id,fingerprint)
        unionIdFingerView(spark, idFingersInMemory.collect(), finalUniqueArticlesArray.map(x => (x.getInformation_id, x.getFingerprint)))

        //存ES
        if(ArticleFilterStream.isSaveToES){
          ArticleFilterStream.esUpdate(finalUniqueArticles)
        }

        //存HBase
        ArticleFilterStream.hbaseUpdate(finalUniqueArticlesArray)

        //MySQL更新
        ArticleFilterStream.mysqlUpdate(finalUniqueArticlesArray)
      }
      logger.info("编辑(UPDATE)后，使得与其他文章重复的有{}条", finalDupArticlesArray.length)
      if (finalDupArticlesArray.length > 0) {
        //更新内存表
        subtractIdFingerView(spark, idFingersInMemory.collect(), finalDupArticlesArray.map(x => x.getInformation_id))

        //向Java端发消息，告知删除
        logger.warn("向Java端发消息，告知删除")
        infoModuleService.deleteInformationByBigData(finalDupArticlesArray.map(_.getInformation_id.toString).toList)
        //ES删除
        ArticleES.delete(finalDupArticlesArray.map { x => Long.box(x.getInformation_id) })
        //HBase删除
        val finalDupArray = finalDupArticlesArray.map { x => x.setIs_delete(1); x }
        ArticleFilterStream.hbaseUpdate(finalDupArray)

        //MySQL更新删除
        ArticleFilterStream.mysqlUpdate(finalDupArticlesArray)
      }
    } else {
      logger.info(s"没有数据需要UPDATE", count)
    }

  }

  def deleteArticles(articleRDD: RDD[Article]) = {
    val count = articleRDD.count()
    if (count != 0) {
      logger.info(s"有{}条数据需要Delete", count)
      val updateTime = JavaUtils.getStrDate()

      val deleteInfos = articleRDD.map(a => (a.getInformation_id,updateTime)).collect()
      ArticleES.delete(deleteInfos.map(x => Long.box(x._1)))
      //HBase删除
      ArticleFilterStream.hbaseDelete(deleteInfos)
      //MySQL删除
      ArticleFilterStream.mysqlDelete(deleteInfos)
    }
  }

  def unionIdFingerView(spark: SparkSession, idFingers1: Array[(Long, Long)], idFingers2: Array[(Long, Long)]) = {
    import spark.implicits._
    spark.sparkContext.makeRDD(idFingers1.union(idFingers2))
      .toDF(ArticleFilterStream.ARTICLE_ID, ArticleFilterStream.FINGER_PRINT)
      .createOrReplaceTempView(ArticleFilterStream.FINGER_PRINT_TEMP_VIEW)
  }

  def subtractIdFingerView(spark: SparkSession, idFingers: Array[(Long, Long)], deleteInformationIds: Array[Long]) = {
    import spark.implicits._

    spark.sparkContext.makeRDD(idFingers.filterNot(x => deleteInformationIds.contains(x._1)))
      .toDF(ArticleFilterStream.ARTICLE_ID, ArticleFilterStream.FINGER_PRINT)
      .createOrReplaceTempView(ArticleFilterStream.FINGER_PRINT_TEMP_VIEW)
  }

  def genSimHash(articleRDD: RDD[Article], rmDup: Boolean = true): RDD[Article] = {
    val updateTime = JavaUtils.getStrDate
    var withSimHash = articleRDD.filter(x => x != null)
      .map(x => (SimHash.simhash64(x.content), x))
    if (rmDup) {
      withSimHash = withSimHash.reduceByKey((x, y) => x)

    }
    withSimHash.map {
      x =>
        x._2.setFingerprint(x._1);
        x._2.setUpd_t(updateTime);
        x._2
    }
  }

  def getIdFingerWithNearHammingDistanceForINSERT(idFingerRDD1: RDD[(Long, Long)], idFingerRDD2: RDD[(Long, Long)]) = {
    idFingerRDD1.cartesian(idFingerRDD2)
      .map(x => (x._1, x._2, SimHash.hammingDistance(x._1._2, x._2._2)))
      .filter(x => x._3 < nearDuplicateDistance)
  }

  def getIdFingerWithNearHammingDistanceForUPDATE(idFingerRDD1: RDD[(Long, Long)], idFingerRDD2: RDD[(Long, Long)]) = {

    idFingerRDD1.cartesian(idFingerRDD2)
      .filter(x => x._1._1 != x._2._1)
      .map(x => (x._1, x._2, SimHash.hammingDistance(x._1._2, x._2._2)))
      .filter(x => x._3 < nearDuplicateDistance)
  }

  def getIdFingersInMemory(spark: SparkSession) = {
    import spark.implicits._
    spark.sql(s"select ${ArticleFilterStream.ARTICLE_ID},${ArticleFilterStream.FINGER_PRINT} from ${ArticleFilterStream.FINGER_PRINT_TEMP_VIEW}")
      .map(r => (ScalaUtils.toString(r(0)).toLong, ScalaUtils.toString(r(1)).toLong)).rdd
  }

}


class ArticleFilterStream(ssc: StreamingContext, session: SparkSession) extends Serializable {

  val logger = LoggerFactory.getLogger(getClass())

  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Props.get("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ArticleFilterStream.groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )

  def loadAllFingerPrints() = {
    SparkUtils.loadFromMysql(s"${ArticleFilterStream.ARTICLE_ID},${ArticleFilterStream.FINGER_PRINT}", ArticleFilterStream.ARTICLE_TABLE, "is_delete=0")
      .select(ArticleFilterStream.ARTICLE_ID, ArticleFilterStream.FINGER_PRINT)
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
    var infoModuleService: InfoModuleService = null
    if (ArticleFilterStream.isWrap) infoModuleService = instanceInfoModuleService()
    loadAllFingerPrints()
    val spark = SparkUtils.getOrCreateSparkSession()

    val partitionNum = Props.get("spark.stream.partitions").toInt

    val offsets = SparkUtils.getOffsetMap(ArticleFilterStream.topic, ArticleFilterStream.groupId)

    val mapper = new ObjectMapper()
    //用于删除文章，只传一个information_id.所以类型Long即可
    val articleType: JavaType = mapper.getTypeFactory.constructParametricType(classOf[DBJson[_]], classOf[Article])
    val sc = ssc.sparkContext
    val mapperBV = sc.broadcast(mapper)
    val articleTypeBV = sc.broadcast(articleType)

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(ArticleFilterStream.topic), kafkaParams,offsets))
      .map(x => (KafkaTopicOffset(x.topic(), ArticleFilterStream.groupId, x.partition(), x.offset()), ArticleFilterStream.getDBJSONArticle(mapperBV, articleTypeBV, x.value())))
      .repartition(partitionNum)
      .foreachRDD {
        rdd =>
          if (rdd.isEmpty()) {
            logger.info("no records")
          } else {
            logger.info("has records")
            try {
              import spark.implicits._
              logger.info("等待去重的文章数:{}", rdd.count())

              //处理INSERT：只有爬虫有INSERT
              ArticleFilterStream.insertArticles(spark, rdd.filter(_._2.getEventType.equalsIgnoreCase("INSERT")).map(_._2.getObj), infoModuleService)
              //处理UPDATE：爬虫的UPDATE和原创的UPDATE+INSERT(原创的INSERT传过来也是UPDATE)
              ArticleFilterStream.updateArticles(spark, rdd.filter(_._2.getEventType.equalsIgnoreCase("UPDATE")).map(_._2.getObj), infoModuleService)
              //处理DELETE
              ArticleFilterStream.deleteArticles(rdd.filter(_._2.getEventType.equalsIgnoreCase("DELETE")).map(_._2.getObj))
              SparkUtils.clearAllRDDs()
            } catch {
              case e: Exception =>
                logger.error("文章去重出错了", e)
            }
            SparkUtils.commitKafkaOffset(rdd.map(_._1))
          }
      }
  }
}
