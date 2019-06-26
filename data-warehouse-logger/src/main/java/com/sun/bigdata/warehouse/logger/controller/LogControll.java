package com.sun.bigdata.warehouse.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.bigdata.warehouse.common.constant.GmallConstants;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * title: LogControll
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-25 19:25
 */
@RestController
@ComponentScan(basePackages ="com.sun.bigdata.warehouse.logger.controller")
public class LogControll {

    @Autowired
    KafkaTemplate<String , String> kafkaTemplate ;
    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LogControll.class) ;
    @PostMapping("log")
    public String doLog(@RequestParam("log") String log){
        // 时间戳
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts" , System.currentTimeMillis());
        // 落盘 file
        String jsonString = jsonObject.toJSONString();
        logger.info(jsonObject.toJSONString());
        // 推送到Kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }
        System.out.println(log);
        return "success" ;
    }

    /**
     * 测试使用
     * @return
     */
    @ResponseBody
    @RequestMapping("test")
    public String doTest(){
        return "success" ;
    }

}
