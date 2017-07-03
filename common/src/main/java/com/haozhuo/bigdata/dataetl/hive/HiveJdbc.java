package com.haozhuo.bigdata.dataetl.hive;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.haozhuo.bigdata.dataetl.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJdbc {
    private static final Logger logger = LoggerFactory.getLogger(HiveJdbc.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Connection con;
    private Statement stmt;

    {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            logger.error("Error", e);
        }

    }

    public void connect() {
        try {
            con = DriverManager.getConnection(Props.get("hive.jdbc.uri") + "/" + Props.get("hive.database"), Props.get("hive.username"), "");
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    public void execute(String sql) {
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    public void close() {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            logger.error("Error", e);
        }
    }

    /**
     *
     * @param table 如：user_info
     * @param partition 如：create_date = '2014-09-23'
     * @param records 如：('1101','13575723659','0','1','1')
     */
    public void insert(String table, String partition, List<String> records) {
        StringBuffer sb = new StringBuffer("INSERT INTO TABLE ")
                .append(table)
                .append(" PARTITION (") //user_info PARTITION (create_date = '2014-09-23') VALUES ('1101', '13575723659', '0','1','1','1','1','1','1','1'), ('1101', '13575723659', '0','1','1','1','1','1','1','1')";
                .append(partition)
                .append(") VALUES ");
        int size = records.size();
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                sb.append(records.get(i));

            } else {
                sb.append(records.get(i)).append(",");
            }
        }
        execute(sb.toString());
    }


    public static void main(String[] args) {
        HiveJdbc client = new HiveJdbc();
        client.connect();
        List<String> list =new ArrayList<String>();
        for(int i =0;i<1;i++){
            list.add("('2222', '13575723659', '0','1','1','1','1','1','1','1')");
        }
        System.out.println("开始插入");
        client.insert("user_info", "create_date = '2014-09-23'", list);
        System.out.println("插入ok");
       // String s = "INSERT INTO TABLE user_info PARTITION (create_date = '2014-09-23') VALUES ('1101', '13575723659', '0','1','1','1','1','1','1','1'), ('1101', '13575723659', '0','1','1','1','1','1','1','1')";
       // client.execute(s);
        client.close();
    }
}
