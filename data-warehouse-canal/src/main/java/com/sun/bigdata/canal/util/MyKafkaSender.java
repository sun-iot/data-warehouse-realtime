package com.sun.bigdata.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * title: MyKafkaSender
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-28 21:14
 */
public class MyKafkaSender {
    public static KafkaProducer<String , String> kafkaProducer = null ;

    /**
     * Kafka的producer
     * @return
     */
    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop104:9092,hadoop105:9092,hadoop106:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String , String> producer = null ;
        try {
            producer = new KafkaProducer<String , String>(properties) ;
        }catch (Exception e){
            e.printStackTrace();
        }

        return producer ;
    }

    /**
     * 发送到Kafka
     * @param topic
     * @param msg
     */
    public static void send(String topic, String msg){
        if (kafkaProducer == null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String , String>(topic , msg)) ;
    }

}
