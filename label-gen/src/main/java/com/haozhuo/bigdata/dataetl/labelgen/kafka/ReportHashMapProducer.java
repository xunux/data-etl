package com.haozhuo.bigdata.dataetl.labelgen.kafka;

import com.haozhuo.bigdata.dataetl.Props;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

public class ReportHashMapProducer {
    private String topicName = Props.get("kafka.topic.name");
    public Producer<String, HashMap> producerInstance;

    public ReportHashMapProducer() {
        Properties props = new Properties();
        props.put("client.id", "112");
        props.put("bootstrap.servers", Props.get("kafka.bootstrap.servers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", Props.get("kafka.batch.size"));
        props.put("linger.ms", Props.get("kafka.linger.ms"));
        props.put("buffer.memory", Props.get("kafka.buffer.memory"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.haozhuo.bigdata.rec.kafka.HashMapSerde");
        producerInstance = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public void send(HashMap<String, String> msg) {
        producerInstance.send(new ProducerRecord<String, HashMap>(topicName, null, msg));
    }

    public void close() {
        producerInstance.close();
    }

}

