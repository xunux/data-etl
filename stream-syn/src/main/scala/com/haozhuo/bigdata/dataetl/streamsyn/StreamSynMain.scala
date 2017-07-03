package com.haozhuo.bigdata.dataetl.streamsyn

import com.haozhuo.bigdata.dataetl.spark.SparkUtils

/**
 * Created by LingXin on 6/30/17.
 */
object StreamSynMain {
  def main(args: Array[String]): Unit = {
    val ssc = SparkUtils.createStream()
    val stream = new StreamSyn(ssc)
    stream.run()
    ssc.start()
    ssc.awaitTermination()
  }
}
