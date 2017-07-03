package com.haozhuo.bigdata.dataetl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlCon implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SqlCon.class);
    private static Connection conn = null;

    public SqlCon() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            logger.error("成功加载MySQL驱动！");
        } catch (ClassNotFoundException e) {
            logger.error("找不到MySQL驱动!", e);
        }
    }

    public static Connection getConnection() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = DriverManager.getConnection(Props.get("mysql.connection.url"), Props.get("mysql.connection.user"), Props.get("mysql.connection.password"));
            }
        } catch (SQLException e) {
            logger.error("ERROR", e);
        }
        return conn;
    }

    public static void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("ERROR", e);
        }
    }

}
