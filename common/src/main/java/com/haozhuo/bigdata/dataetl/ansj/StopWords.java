package com.haozhuo.bigdata.dataetl.ansj;

import com.haozhuo.bigdata.dataetl.Props;
import com.haozhuo.bigdata.dataetl.ScalaUtils;
import org.ansj.recognition.impl.StopRecognition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StopWords implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(StopWords.class);

    private StopWords() {
    }

    private static StopRecognition stopWords;

    public static StopRecognition getInstance() {
        if (stopWords == null) {
            List<String> sw = new ArrayList<String>();
            try {
                BufferedReader stopWordFileBr = new BufferedReader(new InputStreamReader(ScalaUtils.readFile(Props.get("ansj.stopwords.path"))));
                String stopWord;
                while ((stopWord = stopWordFileBr.readLine()) != null) {
                    sw.add(stopWord);
                }
                stopWordFileBr.close();
            } catch (Exception e) {
                logger.error("读取停用词失败", e);
            }
            stopWords = new StopRecognition();
            stopWords.insertStopWords(sw);
        }
        return stopWords;
    }
    public static void main(String[] args) {
        StopWords.getInstance();
    }
}
