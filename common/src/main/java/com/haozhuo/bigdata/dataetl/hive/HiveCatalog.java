package com.haozhuo.bigdata.dataetl.hive;

import com.haozhuo.bigdata.dataetl.Props;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;


public class HiveCatalog implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(HiveCatalog.class);
    public final static String delimiter = "#@@#";
    private HiveEndPoint hiveEP;
    private String[] fields;
    private StreamingConnection connection;
    private DelimitedInputWriter writer;

    public HiveCatalog(String table, String[] fields) {
        this(table, fields, null);
    }

    public HiveCatalog(String table, String[] fields, List<String> partitionVals) {
        this.fields = fields;
        hiveEP = new HiveEndPoint(Props.get("hive.metaStoreUri"), Props.get("hive.database"), table, partitionVals);
    }

    private void init() {
        try {
            connection = hiveEP.newConnection(true);
            writer = new DelimitedInputWriter(fields, delimiter, hiveEP);
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    private void close() {
        if (connection != null) {
            connection.close();
        }
    }

    public void save(List<String> records, int transactionNum) {
        init();
        if (records.size() > 0) {
            try {
                //这里的10不能小于bucket的数量
                TransactionBatch txnBatch = connection.fetchTransactionBatch(transactionNum, writer);
                txnBatch.beginNextTransaction();
                for (String record : records) {
                    txnBatch.write(record.getBytes());
                }
                txnBatch.commit();
                txnBatch.close();
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }
        close();
    }

    public void save(List<String> records) {
        save(records, 10);
    }

    public void save(String[] records, int transactionNum) {
        init();
        if (records.length > 0) {
            try {
                //这里的10不能小于bucket的数量
                TransactionBatch txnBatch = connection.fetchTransactionBatch(transactionNum, writer);
                txnBatch.beginNextTransaction();
                for (String record : records) {
                    txnBatch.write(record.getBytes());
                }
                txnBatch.commit();
                txnBatch.close();

            } catch (Exception e) {
                logger.error("Error", e);
            }
        }
        close();
    }

    public void save(String[] records) {
        save(records, 10);
    }

}
