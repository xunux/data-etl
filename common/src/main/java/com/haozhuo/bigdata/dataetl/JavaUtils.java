package com.haozhuo.bigdata.dataetl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaUtils implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);
    public static String replaceLineBreak(String str) {
        Pattern p = Pattern.compile("\\s*|\\n");
        Matcher m = p.matcher(str);
        return m.replaceAll("");
    }

    public static Double toDouble(String str) {
        try {
            str = str.replaceAll("[^\\d.]+", "");
            if (str.isEmpty()) {
                return 0D;
            } else {
                return Double.parseDouble(str);
            }
        } catch (Exception e) {
            return 0D;
        }
    }

    public static String getStrDate(String format) {
        return new SimpleDateFormat(format).format(new Date());
    }

    public static String getStrDate() {
        return getStrDate("yyyy-MM-dd HH:mm:ss");
    }

    public static Date strToDate(String str,String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);//yyyy-MM-dd HH:mm:ss
        Date date;
        try {
             date=sdf.parse(str);
        } catch (ParseException e) {
            logger.info("解析日期出错:{}",e);
            date = new Date();
        }
        return date;
    }
}
