package com.haozhuo.bigdata.dataetl.labelgen.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 现在此类没有地方用到
 */
public class HashMapSerde implements Serializer<HashMap>, Deserializer<HashMap> {
    private static final Logger logger = LoggerFactory.getLogger(HashMapSerde.class);
    @Override
    public HashMap deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, HashMap.class);
        } catch (IOException e) {
            logger.error("Error", e);
        }
        return new HashMap();
    }

    @Override
    public byte[] serialize(String s, HashMap hashMap) {
        try {
            return objectMapper.writeValueAsBytes(hashMap);
        } catch (IOException e) {
            logger.error("Error", e);
        }
        return "".getBytes();
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }
}
