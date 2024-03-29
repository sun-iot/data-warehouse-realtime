package com.sun.bigdata.publish.mapper;

import java.util.List;
import java.util.Map;

/**
 * title: OrderMapper
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-28 20:15
 */
public interface OrderMapper {
    //1 查询当日交易额总数

    public Double selectOrderAmountTotal(String date);


    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
