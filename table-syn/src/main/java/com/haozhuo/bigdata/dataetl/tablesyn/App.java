package com.haozhuo.bigdata.dataetl.tablesyn;

import com.haozhuo.bigdata.dataetl.Props;
import com.haozhuo.bigdata.dataetl.bean.Report;
import com.haozhuo.bigdata.dataetl.bean.User;
import com.haozhuo.bigdata.dataetl.tablesyn.kafka.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该模块在Hive hcatalog在hdfs是HA的情况下会报错。但是hcatalog在spark on yarn的情况下可以运行，
 * 所以改用spark stream来实现。用stream-syn模块替代table-syn
 */

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) {
        Consumer<User> userConsumer = new Consumer<>(Props.get("kafka.topic.name.user"),User.class);
        Consumer<Report> reportConsumer = new Consumer<>(Props.get("kafka.topic.name.reportcontent"),Report.class);
        while(true) {
            try {
                Thread.sleep(3000);
                userConsumer.fetchData();
                reportConsumer.fetchData();
            } catch (InterruptedException e) {
                logger.error("出现错误", e);
            }
        }
    }
}
