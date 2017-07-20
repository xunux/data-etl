package com.haozhuo.bigdata.dataetl.spark

/**
 * Created by LingXin on 7/9/17.
 */
case class KafkaTopicOffset(val topic:String,val groupId:String,val partition:Int,val offset:Long)