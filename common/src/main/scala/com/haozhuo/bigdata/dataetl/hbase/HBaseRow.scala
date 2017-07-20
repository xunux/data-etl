package com.haozhuo.bigdata.dataetl.hbase

import scala.beans.BeanProperty

case class HBaseRow(@BeanProperty val rowKey:String,@BeanProperty val columns:Array[HBaseColumn])


