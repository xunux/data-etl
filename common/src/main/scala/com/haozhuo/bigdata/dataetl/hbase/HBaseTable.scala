package com.haozhuo.bigdata.dataetl.hbase

import scala.beans.BeanProperty


case class HBaseTable(@BeanProperty val table: String, @BeanProperty val rows: Array[HBaseRow])