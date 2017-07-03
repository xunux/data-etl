package com.haozhuo.bigdata.dataetl.articlefilter;

import com.haozhuo.bigdata.dataetl.bean.Article;
import com.haozhuo.bigdata.dataetl.SqlCon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LingXin on 6/16/17.
 */
public class ArticleDAO {
    public void insert(List<Article> articles) {
        if (articles.size() == 0)
            return;
        Connection conn = SqlCon.getConnection();
        try {
            String query = " INSERT INTO article (information_id,fingerprint, content) VALUES (?,?, ?)";
            Statement stmt = conn.createStatement();
            PreparedStatement preparedStmt = conn.prepareStatement(query);
            conn.setAutoCommit(false);
            for (Article article : articles) {
                preparedStmt.setLong(1, article.information_id());
                preparedStmt.setLong(2, article.fingerprint());
                preparedStmt.setString(3, article.content());

                preparedStmt.addBatch();

            }
            preparedStmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ArrayList<Article> list = new ArrayList<>(10000);
        ArticleDAO dao = new ArticleDAO();
        for (int i = 400000; i < 800000; i++) {
            list.add(new Article(i, i, null, null, null, null, "content", null, null, null, null, null, 0));
            if (i % 10000 == 0) {
                dao.insert(list);
                list = new ArrayList<>(10000);
            }
        }
        dao.insert(list);
    }

}
