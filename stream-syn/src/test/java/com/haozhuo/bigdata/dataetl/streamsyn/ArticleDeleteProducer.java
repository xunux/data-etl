package com.haozhuo.bigdata.dataetl.streamsyn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by LingXin on 6/29/17.
 */
public class ArticleDeleteProducer {
    public static void main(String[] args) {
        String topicName = "dev-syn-table-article";
        Properties props = new Properties();
        props.put("client.id", "11");
        props.put("bootstrap.servers", "192.168.1.152:9092,192.168.1.153:9092");
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


        producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"DELETE\",\"table\":\"article\",\"obj\":{\"information_id\":15915}}"));

        producer.close();

    }
}
