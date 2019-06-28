package com.sun.bigdata.warehouse.app

import com.alibaba.fastjson.JSON
import com.sun.bigdata.warehouse.common.constant.GmallConstants
import com.sun.bigdata.warehouse.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * title: RealtimeStartupApp 
  * projectName data-warehouse-realtime 
  * description: 
  * author Sun-Smile 
  * create 2019-06-28 18:39 
  */
object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeStartupApp")
   // val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    startupStream.map(_.value()).foreachRDD { rdd =>
      println(rdd.collect().mkString("\n"))
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
