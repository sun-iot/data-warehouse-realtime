package com.sun.bigdata.warehouse.app

import com.sun.bigdata.warehouse.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * title: KafkaTest 
  * projectName data-warehouse-realtime 
  * description: 
  * author Sun-Smile 
  * create 2019-06-28 16:47 
  */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaTest").setMaster("local[*]")
    val ssc = new StreamingContext(conf , Seconds(5))
    val kafkaTest: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("atguigu" , ssc )
    kafkaTest.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
