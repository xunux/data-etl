package com.haozhuo.bigdata.dataetl.labelgen.spark.stream

import java.sql.{SQLException, PreparedStatement, Connection}

import com.haozhuo.bigdata.dataetl.ScalaUtils
import com.haozhuo.bigdata.dataetl.bean.User
import com.haozhuo.bigdata.dataetl.hbase.{HBaseTable, HBaseClient, HBaseColumn, HBaseRow}
import com.haozhuo.bigdata.dataetl.mysql.DataSource
import org.slf4j.LoggerFactory

/**
 * Created by LingXin on 7/18/17.
 */
object ParseUser extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())
}

class ParseUser extends Serializable {
  val logger = LoggerFactory.getLogger(getClass())

  def mysqlInsertOrUpdate(users: Array[User]) {

    if (users.length == 0) return
    logger.info("Mysql的user表中需要insert或update{}条数据", users.length)
    var conn: Connection = null
    var preparedStmt: PreparedStatement = null
    val query: String = "INSERT INTO user (user_id, mobile, sex, is_married, has_born, device_model, city, birthday, last_update_time,id_card_no_md5,upd_t) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?,?,?) ON DUPLICATE KEY UPDATE  mobile=?, sex=?, is_married=?, has_born=?, device_model=?, city=?, birthday=?, last_update_time=?,id_card_no_md5=?"
    try {
      conn = DataSource.getInstance.getConnection
      preparedStmt = conn.prepareStatement(query)
      conn.setAutoCommit(false)
      var i: Int = 0
      while (i < users.length) {
        val user: User = users(i)
        preparedStmt.setString(1, user.getUserId)
        preparedStmt.setString(2, user.getMobile)
        preparedStmt.setString(3, user.getSex)
        preparedStmt.setInt(4, user.getIsMarried)
        preparedStmt.setInt(5, user.getHasBorn)
        preparedStmt.setString(6, user.getDeviceModel)
        preparedStmt.setString(7, user.getCity)
        preparedStmt.setString(8, user.getBirthday)
        preparedStmt.setString(9, user.getLastUpdateTime)
        preparedStmt.setString(10, user.getIdCardNoMd5)
        preparedStmt.setString(11, user.getUpdateTime)
        preparedStmt.setString(12, user.getMobile)
        preparedStmt.setString(13, user.getSex)
        preparedStmt.setInt(14, user.getIsMarried)
        preparedStmt.setInt(15, user.getHasBorn)
        preparedStmt.setString(16, user.getDeviceModel)
        preparedStmt.setString(17, user.getCity)
        preparedStmt.setString(18, user.getBirthday)
        preparedStmt.setString(19, user.getLastUpdateTime)
        preparedStmt.setString(20, user.getIdCardNoMd5)
        preparedStmt.addBatch
        i += 1
      }
      preparedStmt.executeBatch
      conn.commit
      conn.setAutoCommit(true)
    } catch {
      case e: Exception => {
        logger.info("Mysql insertOrUpdate USER {} 失败", e)
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

  def hbaseInsertOrUpdate(users: Array[User]) = {
    val array = users.map {
      x =>
        HBaseRow(
          x.getUserId,
          Array(
            HBaseColumn("bd", ScalaUtils.toString(x.getBirthday)),
            HBaseColumn("city", ScalaUtils.toString(x.getCity)),
            HBaseColumn("dev", ScalaUtils.toString(x.getDeviceModel)),
            HBaseColumn("born", ScalaUtils.toString(x.getHasBorn)),
            HBaseColumn("id_card", ScalaUtils.toString(x.getIdCardNoMd5)),
            HBaseColumn("mar", ScalaUtils.toString(x.getIsMarried)),
            HBaseColumn("lup_t", ScalaUtils.toString(x.getLastUpdateTime)),
            HBaseColumn("mobile", ScalaUtils.toString(x.getMobile)),
            HBaseColumn("sex", ScalaUtils.toString(x.getSex)),
            HBaseColumn("upd_t", x.getUpdateTime)
          ).filter(_.value != "")
        )
    }
    logger.info("HBase中的DATAETL:USER_INFO插入数据")
    new HBaseClient().putInto(new HBaseTable("DATAETL:USER_INFO", array))
  }
}
