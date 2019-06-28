package com.sun.bigdata.publish.service.impl;

import com.sun.bigdata.publish.mapper.DauMapper;
import com.sun.bigdata.publish.mapper.OrderMapper;
import com.sun.bigdata.publish.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * title: PublisherServiceImpl
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-28 20:13
 */
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper ;
    @Autowired
    OrderMapper orderMapper ;
    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> mapList = dauMapper.selectDauHourMap(date);
        HashMap hashMap = new HashMap();
        for (Map map : mapList) {
            hashMap.put(map.get("LOGHOUR") , map.get("CT"));
        }
        return hashMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        HashMap map = new HashMap();
        for (Map map1 : mapList) {
            map.put(map1.get("CREATE_HOUR") , map.get("SUM_AMOUNT"));
        }
        return map;
    }
}
