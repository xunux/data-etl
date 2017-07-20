package com.haozhuo.bigdata.dataetl.hbase

import com.haozhuo.bigdata.dataetl.Props
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{SingleColumnValueFilter, FilterList}
import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConversions._


/**
 * Created by LingXin on 7/8/17.
 * http://hbase.apache.org/0.94/book/client.html
 */
class HBaseClient {
  private val logger: Logger = LoggerFactory.getLogger(classOf[HBaseClient])

  private def getConf: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", Props.get("hbase.zookeeper.quorum"))
    return conf
  }

  def getPut(rowkey: String, columns: Array[HBaseColumn]): Put = {
    val thePut: Put = new Put(Bytes.toBytes(rowkey))
    for (column <- columns) {
      thePut.addColumn(Bytes.toBytes(column.getFamily), Bytes.toBytes(column.getField), Bytes.toBytes(column.getValue))
    }
    return thePut
  }

  /**
   * 只允许插入某个表的数据
   */
  def putInto(hbaseTable: HBaseTable) {
    var connection: Connection = null
    var mutator: BufferedMutator = null
    try {
      connection = ConnectionFactory.createConnection(getConf)
      mutator = connection.getBufferedMutator(TableName.valueOf(hbaseTable.getTable))
      val rows: Array[HBaseRow] = hbaseTable.getRows
      for (row <- rows) {
        mutator.mutate(getPut(row.getRowKey, row.getColumns))
      }
    }
    catch {
      case e: Exception => {
        logger.error("往Hbase插入数据出现错误", e)
      }
    } finally {
      try {
        mutator.flush
        mutator.close
        connection.close
      }
      catch {
        case e: Exception => {
          logger.error("关闭HBase客户端连接出现错误", e)
        }
      }
    }
  }

  def dateFilter(cf: String, column: String, startTime: String, endTime: String) = {
    val list: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    val startTimeFilter: SingleColumnValueFilter = new SingleColumnValueFilter(
      Bytes.toBytes(cf),
      Bytes.toBytes(column),
      CompareOp.GREATER_OR_EQUAL,
      Bytes.toBytes(startTime)
    )
    list.addFilter(startTimeFilter)
    val endTimeFilter: SingleColumnValueFilter = new SingleColumnValueFilter(
      Bytes.toBytes(cf),
      Bytes.toBytes(column),
      CompareOp.LESS_OR_EQUAL,
      Bytes.toBytes(endTime)
    )
    list.addFilter(endTimeFilter)
    list
  }

  def scan(tableName: String, cf: String): Unit = {
    val startTime = "2017-07-13 11:22:00"
    val endTime = "2017-07-13 19:23:00"
    val filterColumn = "upd_t"
    val config = getConf

    val connection: Connection = ConnectionFactory.createConnection(config)
    val table: Table = connection.getTable(TableName.valueOf(tableName))

    val scan: Scan = new Scan()

    //scan.setFilter(dateFilter(cf, filterColumn, startTime, endTime))
    scan.setCaching(100000)
    val scanner = table.getScanner(scan)
    try {
      val rs: ResultScanner = table.getScanner(scan)
      var r: Result = rs.next()
      var i = 1;
      while (r != null) {
        print(Bytes.toString(r.getRow)+"   ")
       for(c <- r.listCells()) {
         print(Bytes.toString(CellUtil.cloneValue(c))+"   ")
       }
        println(i+" !!!!!!!!!!!!!!!!!!!!!!!!")
        i=i+1
        r = rs.next()
      }

      rs.close();

    }
    finally {
      table.close()
      scanner.close()
      connection.close()
    }
  }
}


