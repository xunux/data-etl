package com.haozhuo.bigdata.dataetl.hbase

import scala.beans.BeanProperty

/**
 * Created by LingXin on 7/8/17.
 */

case class HBaseColumn(@BeanProperty field:String,@BeanProperty val value: String, @BeanProperty  val family:String="CF")