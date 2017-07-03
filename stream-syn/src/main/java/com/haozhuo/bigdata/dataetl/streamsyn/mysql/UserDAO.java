package com.haozhuo.bigdata.dataetl.streamsyn.mysql;

import com.haozhuo.bigdata.dataetl.SqlCon;
import com.haozhuo.bigdata.dataetl.bean.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

public class UserDAO implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(UserDAO.class);
    public static void insert(List<User> users) {
        if (users.size() == 0)
            return;
        String query = " INSERT INTO user (user_id, mobile,sex,is_married,has_born,device_model,city,birthday,last_update_time,id_card_no_md5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        for (int i = 0; i < users.size(); i++) {
            try {
                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                User user = users.get(i);
                preparedStmt.setString(1, user.getUserId());
                preparedStmt.setString(2, user.getMobile());
                preparedStmt.setString(3, user.getSex());
                preparedStmt.setInt(4, user.getIsMarried());
                preparedStmt.setInt(5, user.getHasBorn());
                preparedStmt.setString(6, user.getDeviceModel());
                preparedStmt.setString(7, user.getCity());
                preparedStmt.setString(8, user.getBirthday());
                preparedStmt.setString(9, user.getLastUpdateTime());
                preparedStmt.setString(10, user.getIdCardNoMd5());
                preparedStmt.execute();
                logger.info("Mysql insert USER:{}",user.getUserId());
                stmt.close();
            } catch (Exception e) {
                logger.info("Mysql insert USER {} 失败", users.get(i).getUserId(), e);
            }
        }
    }

    public static void insertOrUpdate(User[] users) {
         if(users.length == 0)
             return;
        String query = "INSERT INTO user (user_id, mobile, sex, is_married, has_born, device_model, city, birthday, last_update_time,id_card_no_md5) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?,?) ON DUPLICATE KEY UPDATE  mobile=?, sex=?, is_married=?, has_born=?, device_model=?, city=?, birthday=?, last_update_time=?,id_card_no_md5=?";
        for (int i = 0; i < users.length; i++) {
            try {
                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                User user = users[i];
                preparedStmt.setString(1, user.getUserId());
                preparedStmt.setString(2, user.getMobile());
                preparedStmt.setString(3, user.getSex());
                preparedStmt.setInt(4, user.getIsMarried());
                preparedStmt.setInt(5, user.getHasBorn());
                preparedStmt.setString(6, user.getDeviceModel());
                preparedStmt.setString(7, user.getCity());
                preparedStmt.setString(8, user.getBirthday());
                preparedStmt.setString(9, user.getLastUpdateTime());
                preparedStmt.setString(10, user.getIdCardNoMd5());
                preparedStmt.setString(11, user.getMobile());
                preparedStmt.setString(12, user.getSex());
                preparedStmt.setInt(13, user.getIsMarried());
                preparedStmt.setInt(14, user.getHasBorn());
                preparedStmt.setString(15, user.getDeviceModel());
                preparedStmt.setString(16, user.getCity());
                preparedStmt.setString(17, user.getBirthday());
                preparedStmt.setString(18, user.getLastUpdateTime());
                preparedStmt.setString(19, user.getIdCardNoMd5());
                preparedStmt.execute();
                stmt.close();
            } catch (Exception e) {
                logger.info("Mysql insertOrUpdate USER {} 失败", users[i].getUserId(), e);
            }
        }
        logger.info("{} 个user insert or update 成功", users.length);

    }
    public static void update(List<User> users) {
        if (users.size() == 0)
            return;
        String query = "UPDATE user SET mobile=?,sex=?,is_married=?,has_born=?,device_model=?,city=?,birthday=?,last_update_time=?,id_card_no_md5=? WHERE user_id = ?";
        for (int i = 0; i < users.size(); i++) {
            try {
                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                User user = users.get(i);
                preparedStmt.setString(1, user.getMobile());
                preparedStmt.setString(2, user.getSex());
                preparedStmt.setInt(3, user.getIsMarried());
                preparedStmt.setInt(4, user.getHasBorn());
                preparedStmt.setString(5, user.getDeviceModel());
                preparedStmt.setString(6, user.getCity());
                preparedStmt.setString(7, user.getBirthday());
                preparedStmt.setString(8, user.getLastUpdateTime());
                preparedStmt.setString(9, user.getIdCardNoMd5());
                preparedStmt.setString(10, user.getUserId());
                preparedStmt.execute();
                logger.info("Mysql update USER:{}",user.getUserId());
                stmt.close();
            } catch (Exception e) {
                logger.info("Mysql update USER {} 失败", users.get(i).getUserId(), e);
            }
        }
    }
}
