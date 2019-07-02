package com.sun.bigdata.warehouse.app

import com.alibaba.fastjson.JSON
import com.sun.bigdata.warehouse.bean.OrderInfo
import com.sun.bigdata.warehouse.common.constant.GmallConstants
import com.sun.bigdata.warehouse.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
import org.apache.phoenix.spark._

/**
  * @author SunYang
  * @date 2019/07/01
  */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf , streaming.Seconds(5))

    val kafkaDStreamIn: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER , ssc)

    val orderInfoMap: DStream[OrderInfo] = kafkaDStreamIn.map {
      records => {
        // 做JSON数据格式的转换 , 按照OrderInfo的保存
        val orderInfo: OrderInfo = JSON.parseObject(records.value(), classOf[OrderInfo])
        // 提取时间 ， 并对时间做解析
        val timeArray: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = timeArray(0)
        orderInfo.create_hour = timeArray(1)
        // 电话脱敏
        val telTupe: (String, String) = orderInfo.consignee_tel.splitAt(7)
        orderInfo.consignee_tel = "*******" + telTupe._2
        orderInfo
      }
    }
    // 保存到HBase
    orderInfoMap.foreachRDD{
      rdd =>{
        rdd.saveToPhoenix(
          "order_info",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          new Configuration,
          Some("192.168.1.104,192.168.1.105,192.168.1.106:2181") )
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
