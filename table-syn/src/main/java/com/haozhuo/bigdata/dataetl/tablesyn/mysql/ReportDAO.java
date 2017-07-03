package com.haozhuo.bigdata.dataetl.tablesyn.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.haozhuo.bigdata.dataetl.SqlCon;
import com.haozhuo.bigdata.dataetl.bean.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

public class ReportDAO {
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ReportDAO.class);
    public static void insert(List<Report> reports) {
        if (reports.size() == 0)
            return;
        String query = " INSERT INTO report (health_report_id, user_id, id_card_no_md5, birthday,sex,check_unit_code,check_unit_name,report_content,check_date,last_update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        for (int i = 0; i < reports.size(); i++) {
            try {
                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                Report report = reports.get(i);
                preparedStmt.setLong(1, report.getHealthReportId());
                preparedStmt.setString(2, report.getUserId());
                preparedStmt.setString(3, report.getIdCardNoMd5());
                preparedStmt.setString(4, report.getBirthday());
                preparedStmt.setString(5, report.getSex());
                preparedStmt.setString(6, report.getCheckUnitCode());
                preparedStmt.setString(7, report.getCheckUnitName());
                preparedStmt.setString(8, mapper.writeValueAsString(report.getReportContent()));
                preparedStmt.setString(9, report.getCheckDate());
                preparedStmt.setString(10, report.getLastUpdateTime());

                preparedStmt.execute();
                logger.info("Mysql insert REPORT:{}",report.getHealthReportId());
                stmt.close();
            } catch (Exception e) {
                logger.info("Error", e);
            }
        }
    }

    public static void update(List<Report> reports) {
        if (reports.size() == 0)
            return;
        String query = "UPDATE report SET user_id = ?, id_card_no_md5 = ?,birthday=?,sex=?,check_unit_code=?,check_unit_name=?,report_content=?,check_date=?,last_update_time=? WHERE health_report_id = ?";
        for (int i = 0; i < reports.size(); i++) {
            try {
                Connection conn = SqlCon.getConnection();
                Statement stmt = conn.createStatement();
                Report report = reports.get(i);
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, report.getUserId());
                preparedStmt.setString(2, report.getIdCardNoMd5());
                preparedStmt.setString(3, report.getBirthday());
                preparedStmt.setString(4, report.getSex());
                preparedStmt.setString(5, report.getCheckUnitCode());
                preparedStmt.setString(6, report.getCheckUnitName());
                preparedStmt.setString(7, mapper.writeValueAsString(report.getReportContent()));
                preparedStmt.setString(8, report.getCheckDate());
                preparedStmt.setString(9, report.getLastUpdateTime());
                preparedStmt.setLong(10, report.getHealthReportId());
                preparedStmt.execute();
                logger.info("Mysql update REPORT:{}",report.getHealthReportId());
                stmt.close();
            } catch (Exception e) {
                logger.info("Error", e);
            }
        }
    }
}
