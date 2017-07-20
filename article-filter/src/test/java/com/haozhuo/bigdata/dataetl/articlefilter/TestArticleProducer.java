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

/*    int i = 0;
     while ((article = reader.readLine()) != null) {
                if (article.trim() != "") {
                    i = i + 1;
                    producer.send(new ProducerRecord<String, String>(topicName, null, "{\"title\":\"xxx\",\"abstracts\":\"xxx\",\"content\":\"" + article + "\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\"}"));
                }
            }*/
            //System.out.println("总共发送" + i + "篇文章");
           // producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"INSERT\",\"table\":\"article\",\"obj\":{\"title\":\"xxx\",\"abstracts\":\"xxx\",\"content\":\"。老鹰已经与。\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"原创\"}}"));
           // producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"INSERT\",\"table\":\"article\",\"obj\":{\"abstracts\":\"xxx\",\"content\":\"xxxxxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"爬虫\"}}"));
            //producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"UPDATE\",\"table\":\"article\",\"obj\":{\"information_id\":23,\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"去年的G20杭州峰会上，中国针对这些问题提出了全球经济治理的新主张，并协调G20各方一致通过了以科技创新、结构性改革、新工业革命和数字经济为核心的《二十国集团创新增长蓝图》，同时第一次把发展问题置于全球宏观政策框架的突出位置；第一次就落实联合国2030年可持续发展议程制定行动计划；第一次集体支持非洲和最不发达国家工业化。这在二十国集团历史上具有重要开创性意义。\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"爬虫\"}}"));
           // producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"INSERT\",\"table\":\"article\",\"obj\":{\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"wo shi yi zhi xiao ping guo\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性,女性\",\"data_source\":\"爬虫\"}}"));
            //producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"UPDATE\",\"table\":\"article\",\"obj\":{\"information_id\":17,\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"ha ha xx oo\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性,女性\",\"data_source\":\"爬虫\"}}"));


            // producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"UPDATE\",\"table\":\"article\",\"obj\":{\"information_id\":23784,\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"鲁迪-盖伊今夏以2年1700万美金加盟圣安东尼奥马刺队，新赛季在马刺队这位老将期待迎来最好的回归。\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"爬虫\"}}"));
            /*producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"UPDATE\",\"table\":\"article\",\"obj\":{\"information_id\":19,\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"随着小牛在夏联半决赛中不敌湖人，对于丁彦雨航来说，今年的NBA夏季联赛之旅已经正式结束。5场奥兰多夏季联赛，6场拉斯维加斯夏季联赛，一共11场比赛，丁彦雨航场均6.9分2.6篮板1.1助攻，投篮命中率41%，三分命中率仅有22.7%。显然，他的投篮，特别是三分球，仍需苦练。\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"爬虫\"}}"));
            producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"UPDATE\",\"table\":\"article\",\"obj\":{\"information_id\":20,\"title\":\"pc\",\"abstracts\":\"xxx\",\"content\":\"随着小牛在夏联半决赛中不敌湖人，对于丁彦雨航来说，今年的NBA夏季联赛之旅已经正式结束。5场奥兰多夏季联赛，6场拉斯维加斯夏季联赛，一共11场比赛，丁彦雨航场均6.9分2.6篮板1.1助攻，投篮命中率41%，三分命中率仅有22.7%。显然，他的投篮，特别是三分球，仍需苦练。\",\"source\":\"xxx\",\"htmls\":\"xxx\",\"create_time\":\"2017-11-11 11:11:11\",\"crawler_time\":\"2017-11-11 11:11:11\",\"image_thumbnail\":\"imge\",\"image_list\":\"img_list\",\"display_url\":\"www.baidu.com\",\"comment_count\":0,\"news_category\":\"男性\",\"data_source\":\"爬虫\"}}"));
*/
            //DELETE
            //producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"DELETE\",\"table\":\"article\",\"obj\":{\"information_id\":17}}"));
            producer.send(new ProducerRecord<String, String>(topicName, null, "{\"eventType\":\"DELETE\",\"table\":\"article\",\"obj\":{\"information_id\":17}}"));
            producer.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

