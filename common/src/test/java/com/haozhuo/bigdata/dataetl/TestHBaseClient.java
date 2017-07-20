package com.haozhuo.bigdata.dataetl;

import com.haozhuo.bigdata.dataetl.hbase.HBaseClient;

/**
 * Created by LingXin on 7/13/17.
 */
public class TestHBaseClient {
    public static void main(String[] args) {
        HBaseClient client = new HBaseClient();
        client.scan("DATAETL:RPT_IND","CF");
    }
}
