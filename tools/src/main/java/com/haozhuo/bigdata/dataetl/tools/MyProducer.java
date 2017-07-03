package com.haozhuo.bigdata.dataetl.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
    public static void main(String args[]) {
        System.out.println("topicName:"+args[0]);
        String topicName = args[0];
        Properties props = new Properties();
        props.put("client.id", "113");
        props.put("bootstrap.servers", "datanode1:9092,datanode2:9092,datanode3:9092");
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
        producer.send(new ProducerRecord<String, String>(topicName, null, args[1]));
        System.out.println("Message sent successfully");
        producer.close();
    }
}
