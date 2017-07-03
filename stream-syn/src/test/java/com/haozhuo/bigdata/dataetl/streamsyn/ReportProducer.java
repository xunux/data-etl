package com.haozhuo.bigdata.dataetl.streamsyn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ReportProducer {
    public static void main(String args[]) {
        String topicName = "dev-syn-table-reportcontent";
        Properties props = new Properties();
        props.put("client.id", "113");
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
        producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"insert\",\"obj\":{\"birthday\":\"1984-08-30\",\"checkDate\":\"2014-12-21\",\"checkUnitCode\":\"美年大健康南京秦淮分院\",\"checkUnitName\":\"美年大健康分院\",\"healthReportId\":13333,\"idCardNoMd5\":\"6a3e1597ca748a40ee2be5c91589b31f\",\"lastUpdateTime\":\"2017-04-11 10:45:42\",\"reportContent\":{\"checkItems\":[{\"checkItemName\":\"颈椎侧位检查\",\"checkResults\":[{\"appendInfo\":\"\",\"canExplain\":0,\"checkIndexName\":\"X光片号\",\"highValueRef\":\"\",\"lowValueRef\":\"\",\"resultFlagId\":1,\"resultValue\":\"\",\"textRef\":\"\",\"unit\":\"\"},{\"appendInfo\":\"\",\"canExplain\":0,\"checkIndexName\":\"描述\",\"highValueRef\":\"\",\"lowValueRef\":\"\",\"resultFlagId\":1,\"resultValue\":\"颈椎诸椎体排列整齐，生理曲度存在，各椎体及附件对应关系正常，骨质未见破坏及增生，>椎间孔大小形态正常，椎间隙宽窄如常，前后纵韧带无钙化。\",\"textRef\":\"\",\"unit\":\"\"},{\"appendInfo\":\"\",\"canExplain\":0,\"checkIndexName\":\"检查结果\",\"highValueRef\":\"\",\"lowValueRef\":\"\",\"resultFlagId\":1,\"resultValue\":\"颈椎DR检查未见异常\",\"textRef\":\"\",\"unit\":\"\"}],\"checkUserName\":\"朱牧\",\"departmentName\":\"\"},{\"checkItemName\":\"血清总胆固醇 (TC)\",\"checkResults\":[{\"appendInfo\":\"\",\"canExplain\":1,\"checkIndexName\":\"血清总胆固醇\",\"highValueRef\":\"\",\"lowValueRef\":\"\",\"resultFlagId\":1,\"resultValue\":\"4.7\",\"textRef\":\"3.1-6.1\",\"unit\":\"mmol/L\"}],\"checkUserName\":\"陈先迪\",\"departmentName\":\"\"}],\"generalSummarys\":[{\"fw\":\"100-300 10^9/L\",\"result\":\"342.00\",\"reviewAdvice\":\"\",\"summaryAdvice\":\"血小板计数异常，明原因。\\\\n\",\"summaryDescription\":\"\",\"summaryMedicalExplanation\":\"血小量与止血和凝血功能密切相关。\",\"summaryName\":\"血小板计数 增高\",\"summaryReasonResult\":\"血小板增多见发生血栓性疾病。\"}],\"generalSummarys2\":[\"★ 您好，欢迎您来到美年大医疗健康服务。您本次体检结果如下:\\\\n\\\\n\",\"★ 一般检查（体重身高）:\\\\n  体重随访复 查。\\\\n\\\\n\",\"★ 内异常\\\\n\\\\n\",\"★ 外科检查结果:\\\\n  外科检查未见异常。\\\\n\\\\n\",\"★ 眼科检查。\\\\n\\\\n\",\"★ 耳鼻喉检查\\\\n\",\"★ 心电图检\\n\\\\n\",\"★ 彩超明显异常\\\\n\\\\n\",\"★ 胸部正位\",\"★ 颈椎侧\",\"★ 血小板偏高：可能与血液阴道炎，建议择期复查。\\\\n\\\\n\"]},\"sex\":\"男\",\"userId\":\"12345\"},\"table\":\"report\"}"));

        System.out.println("Message sent successfully");
        producer.close();
    }
}
