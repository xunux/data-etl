package com.haozhuo.bigdata.dataetl.bean;

import java.io.Serializable;

public class DBJson<T> implements Serializable {
    String table;
    String eventType;
    T obj;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

}
