package com.haozhuo.bigdata.dataetl.mysql;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import com.haozhuo.bigdata.dataetl.Props;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http://www.javatips.net/blog/c3p0-connection-pooling-example
 */
public class DataSource implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DataSource.class);
    private static DataSource datasource;
    private ComboPooledDataSource cpds;

    private DataSource() throws IOException, SQLException, PropertyVetoException {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl(Props.get("mysql.connection.url"));
        cpds.setUser(Props.get("mysql.connection.user"));
        cpds.setPassword(Props.get("mysql.connection.password"));
        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(1);
        cpds.setAcquireIncrement(1);
        cpds.setMaxPoolSize(4);
        cpds.setMaxStatements(100);
    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
        if (datasource == null) {
            logger.info("初始化c3p0连接池");
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.cpds.getConnection();
    }
}
