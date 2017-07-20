package com.haozhuo.bigdata.dataetl.labelgen.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class UserProducer {
    public static void main(String args[]) {
        String topicName = "dev-syn-table-user";
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

      // producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"insert\",\"obj\":{\"city\":\"市\",\"deviceModel\":\"iPhone_6Plus\",\"hasBorn\":0,\"isMarried\":0,\"lastUpdateTime\":\"2017-06-14 10:23:16\",\"mobile\":\"13588249303****\",\"sex\":\"女\",\"userId\":\"a522-e85876b7972c\"},\"table\":\"user\"}"));
        int i = 1;
        while(i<100){
            producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"INSERT\",\"obj\":{\"birthday\":\"1990-01-01\",\"city\":\"杭州市\",\"deviceModel\":\"Redmi Note 4\",\"hasBorn\":1,\"isMarried\":1,\"lastUpdateTime\":\"2017-06-16 16:53:49\",\"mobile\":\"14300000001****\",\"sex\":\"男\",\"userId\":\""+i+"\"},\"table\":\"user\"}"));
            System.out.println(i);
            i++;
        }

        producer.close();
    }
}
