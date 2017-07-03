package com.haozhuo.bigdata.dataetl.tablesyn.hive;

import com.haozhuo.bigdata.dataetl.JavaUtils;
import com.haozhuo.bigdata.dataetl.bean.Report;
import com.haozhuo.bigdata.dataetl.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LingXin on 6/27/17.
 */
public class ReportCatalog implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ReportCatalog.class);

    public static void insert(List<Report> reports) {
        if (reports.size() == 0)
            return;
        logger.info("{}个reports开始插入Hive", reports.size());
        ArrayList<String> partitionVals = new ArrayList<String>(1);
        partitionVals.add(JavaUtils.getStrDate("yyyy-MM-dd"));


        HiveCatalog hiveCatalog = new HiveCatalog("report", new String[]{"health_report_id", "birthday", "check_date", "check_unit_code", "check_unit_name", "id_card_no_md5", "last_update_time", "report_content", "sex", "user_id"}, partitionVals);
        List<String> reportStrs = new ArrayList<String>(reports.size());
        for (Report report : reports) {
            reportStrs.add(new StringBuffer().append(report.getHealthReportId()).append(HiveCatalog.delimiter)
                    .append(report.getBirthday()).append(HiveCatalog.delimiter)
                    .append(report.getCheckDate()).append(HiveCatalog.delimiter)
                    .append(report.getCheckUnitCode()).append(HiveCatalog.delimiter)
                    .append(report.getCheckUnitName()).append(HiveCatalog.delimiter)
                    .append(report.getIdCardNoMd5()).append(HiveCatalog.delimiter)
                    .append(report.getLastUpdateTime()).append(HiveCatalog.delimiter)
                    .append(report.getReportContent()).append(HiveCatalog.delimiter)
                    .append(report.getSex()).append(HiveCatalog.delimiter)
                    .append(report.getUserId()).append(HiveCatalog.delimiter)
                    .toString());
        }
        hiveCatalog.save(reportStrs, 2);
        logger.info("reports插入Hive完成", reports.size());
    }
}
