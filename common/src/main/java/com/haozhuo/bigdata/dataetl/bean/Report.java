package com.haozhuo.bigdata.dataetl.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.haozhuo.bigdata.dataetl.JavaUtils;

import java.io.Serializable;

public class Report implements Serializable {

    @JsonProperty("healthReportId")
    private Long healthReportId;
    @JsonProperty("userId")
    private String userId;
    @JsonProperty("idCardNoMd5")
    private String idCardNoMd5;
    @JsonProperty("birthday")
    private String birthday;
    @JsonProperty("sex")
    private String sex;
    @JsonProperty("checkUnitCode")
    private String checkUnitCode;
    @JsonProperty("checkUnitName")
    private String checkUnitName;
    @JsonProperty("reportContent")
    private Object reportContent;
    @JsonProperty("checkDate")
    private String checkDate;
    @JsonProperty("lastUpdateTime")
    private String lastUpdateTime;
    @JsonProperty("updateTime")
    private String updateTime;

    public String getIdCardNoMd5() {
        return idCardNoMd5;
    }

    public void setIdCardNoMd5(String idCardNoMd5) {
        this.idCardNoMd5 = idCardNoMd5;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public Object getReportContent() {
        return reportContent;
    }

    public void setReportContent(Object reportContent) {
        this.reportContent = reportContent;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Long getHealthReportId() {
        return healthReportId;
    }

    public void setHealthReportId(Long healthReportId) {
        this.healthReportId = healthReportId;
    }

    public String getCheckUnitCode() {
        return checkUnitCode;
    }

    public void setCheckUnitCode(String checkUnitCode) {
        this.checkUnitCode = checkUnitCode;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCheckUnitName() {
        return checkUnitName;
    }

    public void setCheckUnitName(String checkUnitName) {
        this.checkUnitName = checkUnitName;
    }

    public String getCheckDate() {
        return checkDate;
    }

    public void setCheckDate(String checkDate) {
        this.checkDate = checkDate;
    }

    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
