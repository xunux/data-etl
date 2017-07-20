package com.haozhuo.bigdata.dataetl.bean;

import com.haozhuo.bigdata.dataetl.JavaUtils;

import java.io.Serializable;

public class User implements Serializable {
    private String userId;
    private String idCardNoMd5;
    private String city;
    private String deviceModel;
    private int hasBorn;
    private int isMarried;
    private String lastUpdateTime;
    private String mobile;
    private String sex;
    private String birthday;
    private String updateTime = JavaUtils.getStrDate();

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public int getHasBorn() {
        return hasBorn;
    }

    public void setHasBorn(int hasBorn) {
        this.hasBorn = hasBorn;
    }

    public int getIsMarried() {
        return isMarried;
    }

    public void setIsMarried(int isMarried) {
        this.isMarried = isMarried;
    }

    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getIdCardNoMd5() {
        return idCardNoMd5;
    }

    public void setIdCardNoMd5(String idCardNoMd5) {
        this.idCardNoMd5 = idCardNoMd5;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
