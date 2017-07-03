package com.haozhuo.bigdata.dataetl.streamsyn.mysql;

import com.haozhuo.bigdata.dataetl.SqlCon;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Created by LingXin on 6/29/17.
 */
public class ArticleDAO {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ArticleDAO.class);
    public static void delete(Long [] informationIds) {
        System.out.println("ArticleDAO delete" + informationIds.length );
        if (informationIds.length == 0)
            return;
        String query = "UPDATE article SET is_delete = 1 WHERE information_id = ?";
        for (int i = 0; i < informationIds.length; i++) {
            try {

                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setLong(1, informationIds[i]);
                preparedStmt.execute();
                stmt.close();
            } catch (Exception e) {
                logger.info("Error", e);
            }
        }
        logger.info("Mysql delete {} Articles:",informationIds.length);

    }
}
