package com.haozhuo.bigdata.dataetl

import java.io.{FileInputStream, InputStream}
import org.slf4j.LoggerFactory
object ScalaUtils {
  val logger = LoggerFactory.getLogger(getClass())
  def readFile(path: String): InputStream = {
    var in: InputStream = null
    val hdfsPartten = "hdfs://[0-9a-zA_Z.:]+/".r
    val reg = hdfsPartten.findFirstIn(path)
    if (reg.isDefined) {
      logger.info("HDFS命名空间:{}", reg.get)
      logger.info("加载HDFS中的文件:{}", path)
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(reg.get), hadoopConf)
      in = hdfs.open(new org.apache.hadoop.fs.Path(path)).asInstanceOf[java.io.InputStream]
    } else {
      logger.info("加载本地目录下的文件:{}", path)
      in = new FileInputStream(path)
    }
    in
  }

  def toString(any:Any): String = {
    if(any == null)
      ""
    else
      any.toString
  }
}
