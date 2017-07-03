package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import com.haozhuo.bigdata.dataetl.spark.SparkUtils

object StreamParseMain {
  def main(args: Array[String]): Unit = {
    val ssc = SparkUtils.createStream()
    val stream = new StreamParseJson(ssc)
    stream.run()
    ssc.start()
    ssc.awaitTermination()
  }
}
