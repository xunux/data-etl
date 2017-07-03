package com.haozhuo.bigdata.dataetl.labelgen.canal;

/*
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.haozhuo.bigdata.dataetl.Props;
import com.haozhuo.bigdata.dataetl.labelgen.kafka.ReportHashMapProducer;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;*/

public class CanalConsumer {
    /*ReportHashMapProducer producer = new ReportHashMapProducer();

    public void run() {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(Props.get("canal.ip"), Integer.parseInt(Props.get("canal.port"))), Props.get("canal.destinations"), "", "");
        String tables = Props.get("canal.tables");
        int batchSize = 1000;
        try {
            connector.connect();

            //订阅所有表 connector.subscribe(".*\\..*");
            //订阅多张表 connector.subscribe("db.table1,db.table2");
            connector.subscribe(tables);
            connector.rollback();
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } finally {
            connector.disconnect();
        }
    }


    private void printEntry(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChange.getEventType();

            for (RowData rowData : rowChange.getRowDatasList()) {
                try {
                    List<Column> columns;
                    HashMap<String, String> map = new HashMap<>();
                    if (eventType == EventType.DELETE) {
                     //   columns = rowData.getBeforeColumnsList();
                        //  user = new User(Integer.parseInt(columns.get(0).getValue()), null);

                        // producer.send(msg);*//**//*

                    } else if (eventType == EventType.INSERT) {
                        columns = rowData.getAfterColumnsList();
                        map.put("eventType", "INSERT");
                        for (Column column : columns) {
                            map.put(column.getName(), column.getValue());
                        }
                    }
                    System.out.println("向kafka发送：" + map);
                    producer.send(map);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    public static void main(String args[]) {
        CanalConsumer a = new CanalConsumer();
        a.run();
    }*/
}
