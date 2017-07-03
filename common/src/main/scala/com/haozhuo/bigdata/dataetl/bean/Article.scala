package com.haozhuo.bigdata.dataetl.bean

import com.fasterxml.jackson.annotation.JsonProperty

import scala.beans.BeanProperty

/**
 * Created by LingXin on 6/15/17.
 */
case class Article  (
  @BeanProperty @JsonProperty("information_id")  var information_id: Long,
  @BeanProperty @JsonProperty("fingerprint")var fingerprint: Long,
  @BeanProperty @JsonProperty("title") var title: String,
  @BeanProperty @JsonProperty("image_list") var image_list: String,
  @BeanProperty @JsonProperty("image_thumbnail") var image_thumbnail: String,
  @BeanProperty @JsonProperty("abstracts") var abstracts: String,
  @BeanProperty @JsonProperty("content") var content: String,
  @BeanProperty @JsonProperty("source") var source: String,
  @BeanProperty @JsonProperty("display_url")  var display_url: String,
  @BeanProperty @JsonProperty("htmls") var htmls: String,
  @BeanProperty @JsonProperty("create_time")  var create_time: String,
  @BeanProperty @JsonProperty("crawler_time")  var crawler_time: String,
  @BeanProperty @JsonProperty("is_delete")  var is_delete: Int = 0
) extends Serializable






