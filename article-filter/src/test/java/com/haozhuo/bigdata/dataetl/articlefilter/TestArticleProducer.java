package com.haozhuo.bigdata.dataetl.articlefilter;

import com.haozhuo.bigdata.dataetl.Props;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class TestArticleProducer {
    public static void main(String[] args) {
        String topicName = "dev-dataetl-articlefilter";
        Properties props = new Properties();
        props.put("client.id", "11");
        props.put("bootstrap.servers", Props.get("kafka.bootstrap.servers"));
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props);


        String stopWordTable = "/data/work/luciuschina/data-etl/article-filter/src/test/resources/articles";
        File f = new File(stopWordTable);
        FileInputStream fileInputStream;
        try {
            fileInputStream = new FileInputStream(f);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
            String article;
            int i = 0;
            while ((article = reader.readLine()) != null) {
                if (article.trim() != "") {
                    i = i + 1;
                    producer.send(new ProducerRecord<String, String>(topicName, null, "{\"title\":\"xxx\",\"abstracts\":\"xxx\",\"content\":\"" + article + "\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\"}"));

                }
            }
            System.out.println("总共发送" + i + "篇文章");
            producer.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

