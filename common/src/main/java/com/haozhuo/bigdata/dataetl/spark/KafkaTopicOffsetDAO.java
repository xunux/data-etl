package com.haozhuo.bigdata.dataetl.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.haozhuo.bigdata.dataetl.mysql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by LingXin on 7/9/17.
 */
public class KafkaTopicOffsetDAO implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicOffsetDAO.class);

    public void insertOrUpdate(KafkaTopicOffset[] topicOffsets) {
        if (topicOffsets.length == 0)
            return;

        ObjectMapper mapper = new ObjectMapper();
        Connection conn = null;
        PreparedStatement preparedStmt = null;
        String query = "INSERT INTO kafka_topic_offset (topic, group_id, `partition`, `offset`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE offset=?";

        try {
            conn = DataSource.getInstance().getConnection();
            preparedStmt = conn.prepareStatement(query);
            conn.setAutoCommit(false); // 设置手动提交
            for (int i = 0; i < topicOffsets.length; i++) {

                KafkaTopicOffset topicOffset = topicOffsets[i];
                preparedStmt.setString(1, topicOffset.topic());
                preparedStmt.setString(2, topicOffset.groupId());
                preparedStmt.setInt(3, topicOffset.partition());
                preparedStmt.setLong(4, topicOffset.offset());
                preparedStmt.setLong(5, topicOffset.offset());
                preparedStmt.addBatch();
            }
            preparedStmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
            logger.info("Mysql insertOrUpdate KafkaTopicOffsetDAO 失败", e);
        } finally {
            if (preparedStmt != null) try { preparedStmt.close(); } catch (SQLException e) { logger.info("Error", e);}
            if (conn != null) try { conn.close(); } catch (SQLException e) { logger.info("Error", e);}
        }
    }
}
