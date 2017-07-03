package com.haozhuo.bigdata.dataetl.tablesyn.kafka;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.haozhuo.bigdata.dataetl.Props;
import com.haozhuo.bigdata.dataetl.bean.DBJson;
import com.haozhuo.bigdata.dataetl.bean.Report;
import com.haozhuo.bigdata.dataetl.bean.User;
import com.haozhuo.bigdata.dataetl.tablesyn.hive.ReportCatalog;
import com.haozhuo.bigdata.dataetl.tablesyn.hive.UserCatalog;
import com.haozhuo.bigdata.dataetl.tablesyn.mysql.ReportDAO;
import com.haozhuo.bigdata.dataetl.tablesyn.mysql.UserDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer<T> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Class<T> typeParameterClass;
    private KafkaConsumer<String, String> consumer;
    private Properties props = new Properties();
    private ObjectMapper mapper = new ObjectMapper();
    private JavaType myType;
    private String topic;

    public Consumer(String topic, Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
        this.topic = topic;
        props.put("bootstrap.servers", Props.get("kafka.bootstrap.servers"));
        props.put("group.id", topic + "_" + Props.get("kafka.consumer.group.id"));
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        myType = mapper.getTypeFactory().constructParametricType(DBJson.class, typeParameterClass);
        logger.info("bootstrap.servers:{}", props.get("bootstrap.servers"));
        logger.info("topic:{}", topic);
        logger.info("consumer group.id:{}", props.get("group.id"));
    }

    public void fetchData() {
        ConsumerRecords<String, String> records = consumer.poll(100);
        List<T> insertList = new ArrayList<T>();
        List<T> updateList = new ArrayList<T>();
        try {
            logger.info("从{}消费{}条数据", topic, records.count());
            for (ConsumerRecord<String, String> record : records) {
                DBJson<T> obj = null;
                obj = mapper.readValue(record.value(), myType);
                if ("insert".equalsIgnoreCase(obj.getEventType())) {
                    insertList.add(obj.getObj());
                } else if ("update".equalsIgnoreCase(obj.getEventType())) {
                    updateList.add(obj.getObj());
                }
            }
            saveToDB(insertList, updateList);
        } catch (IOException e) {
            logger.error("消息解析时出错", e);
        }
    }

    private void saveToDB(List<T> insertList, List<T> updateList) {
        if (typeParameterClass.isAssignableFrom(User.class)) {
            UserDAO.insertOrUpdate((List<User>) insertList);
            UserDAO.insertOrUpdate((List<User>) updateList);
            UserCatalog.insert((List<User>) insertList);
            //hive中update作为insert插入
            UserCatalog.insert((List<User>) updateList);
        } else if (typeParameterClass.isAssignableFrom(Report.class)) {
            ReportDAO.insert((List<Report>) insertList);
            ReportDAO.update((List<Report>) updateList);
            ReportCatalog.insert((List<Report>) insertList);
        }
    }
}
