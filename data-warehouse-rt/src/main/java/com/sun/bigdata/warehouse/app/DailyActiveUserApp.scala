package com.sun.bigdata.warehouse.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.sun.bigdata.warehouse.bean.StartupLog
import com.sun.bigdata.warehouse.util.{MyKafkaUtil, RedisUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * title: DailyActiveUserApp 
  * projectName data-warehouse-realtime 
  * description: 日活用户
  * author Sun-Smile 
  * create 2019-06-26 19:51 
  */
object DailyActiveUserApp {
  private val conf: SparkConf = new SparkConf().setAppName("DailyActiveUserApp").setMaster("local[*]")
  private val ssc = new StreamingContext(conf, Seconds(5))

  def main(args: Array[String]): Unit = {

    // 得到Kafka消费者的数据源
    val kafkaDStreamIn: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("gmall_start", ssc)
    kafkaDStreamIn.print()
    // 先看看是否连接通
//    kafkaDStreamIn.foreachRDD(
//      rdd => {
//        println(rdd.map(_.value()).collect().mkString("\n"))
//      }
//    )

    // 数据流进行转换
    val startupLogDStream: DStream[StartupLog] = kafkaDStreamIn.map {
      record => {
        val info: String = record.value()
        val startupLog: StartupLog = JSON.parseObject(info, classOf[StartupLog])
        val dateFormat: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))

        val dateArray: Array[String] = dateFormat.split(" ")
        startupLog.logDate = dateArray(0)
        startupLog.logHour = dateArray(1)
        startupLog
      }
    }
    startupLogDStream.cache()

    // 利用用户清单进行过滤，去重，只保留清单中不存在的用户访问记录
    val startupFilterDStream: DStream[StartupLog] = startupLogDStream.transform {
      rdd => {
        // 得到redis的客户端 --> driver
        val client: Jedis = RedisUtils.getRedisClient
        //
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val key = "daily-activite-user:" + dateStr
        val dailyActiveUserdSet: util.Set[String] = client.smembers(key)
        client.close()

        val dailrUserBro: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dailyActiveUserdSet)
        println("过滤前：" + rdd.count())
        // 开始进行过滤
        val filterRDD: RDD[StartupLog] = rdd.filter {
          startup => {
            val value: util.Set[String] = dailrUserBro.value
            !value.contains(startup.mid)
          }
        }
        filterRDD
      }
    }
    val startmidMap: DStream[(String, Iterable[StartupLog])] = startupFilterDStream.map {
      startuplog => {
        (startuplog.mid, startuplog)
      }
    }.groupByKey()
    //   startupFilterDStream.print()
    // 批次内进行去重，按照mid进行分组，每组取一个值

    val distinctDStream: DStream[StartupLog] = startmidMap.flatMap {
      case (mid, startupMid) => {
        startupMid.toList.take(1)
      }
    }

    // 保存今日访问过的用户 mid 清单 ， --> redis : set - dau:2019-xx-xx - mid
    distinctDStream.foreachRDD {
      // driver
      rdd => {
        rdd.foreachPartition {
          startupItr => {
            // excutor
            val client: Jedis = RedisUtils.getRedisClient
            for (startup <- startupItr) {
              val key = "daily-activite-user:" + startup.logDate
              client.sadd(key, startup.mid)
              println("插入redis--" + startup)
            }
            client.close()
          }
        }
      }
    }

    // 对去重后的数据进行写入到HBase
    distinctDStream.foreachRDD{
      rdd=>{
        val configuration = new Configuration()
        println(rdd.collect().mkString("\n"))

        rdd.saveToPhoenix(
          "gmall0105_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          configuration,
          Some("hadoop104,hadoop105,hadoop106:2181"))
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
