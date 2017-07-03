package com.haozhuo.bigdata.dataetl.articlefilter.spark

import com.haozhuo.bigdata.dataetl.spark.SparkUtils


object ArticleFilterApp {
  def main(args: Array[String]) {
    val session = SparkUtils.getOrCreateSparkSession()
    val ssc = SparkUtils.createStream()
    val stream = new ArticleFilterStream(ssc,session)
    stream.run()
    ssc.start()
    ssc.awaitTermination()
  }
}
