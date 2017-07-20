package com.haozhuo.info.entity.vo.param;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by ZhangZuoCong on 2017/6/23.
 */
public class InformationBigDataParam implements Serializable {

    //大数据主键
    private Long fingerprint;

    private Integer informationId;

    private Byte type;

    private Byte status;

    private Byte urlType;

    private String title;

    private String url;

    private String image;

    private String images;

    private String content;

    private String informationLabelsIds;

    private Date startTime;

    private Date endTime;

    //大数据不需要赋值
    private Date createTime;

    //大数据不需要赋值
    private Date lastUpdateTime;

    private String basicLabelIds;

    private String diseaseLabelIds;

    //大数据不需要赋值
    private Byte sourceType;

    //大数据不需要赋值
    private Byte haveDispose;

    private String detail;

    //来源
    private String source;

    //分类
    private String newsCategory;

    private String dataSource;

    static final long serialVersionUID = 1L;

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getNewsCategory() {
        return newsCategory;
    }

    public void setNewsCategory(String newsCategory) {
        this.newsCategory = newsCategory;
    }

    public Long getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(Long fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Integer getInformationId() {
        return informationId;
    }

    public void setInformationId(Integer informationId) {
        this.informationId = informationId;
    }

    public Byte getType() {
        return type;
    }

    public void setType(Byte type) {
        this.type = type;
    }

    public Byte getStatus() {
        return status;
    }

    public void setStatus(Byte status) {
        this.status = status;
    }

    public Byte getUrlType() {
        return urlType;
    }

    public void setUrlType(Byte urlType) {
        this.urlType = urlType;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getImages() {
        return images;
    }

    public void setImages(String images) {
        this.images = images;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getInformationLabelsIds() {
        return informationLabelsIds;
    }

    public void setInformationLabelsIds(String informationLabelsIds) {
        this.informationLabelsIds = informationLabelsIds;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getBasicLabelIds() {
        return basicLabelIds;
    }

    public void setBasicLabelIds(String basicLabelIds) {
        this.basicLabelIds = basicLabelIds;
    }

    public String getDiseaseLabelIds() {
        return diseaseLabelIds;
    }

    public void setDiseaseLabelIds(String diseaseLabelIds) {
        this.diseaseLabelIds = diseaseLabelIds;
    }

    public Byte getSourceType() {
        return sourceType;
    }

    public void setSourceType(Byte sourceType) {
        this.sourceType = sourceType;
    }

    public Byte getHaveDispose() {
        return haveDispose;
    }

    public void setHaveDispose(Byte haveDispose) {
        this.haveDispose = haveDispose;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
}
