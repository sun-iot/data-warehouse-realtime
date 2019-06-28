package com.sun.bigdata.publish.mapper;

import java.util.List;
import java.util.Map;

/**
 * title: DauMapper
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-28 20:16
 */
public interface DauMapper {
    //1 查询日活总数
    // select count(*) ct from gmall0105_dau where logdate=date
    public Long selectDauTotal(String date);


    //2 查询日活分时明细
    // select  loghour,count(*) ct  from gmall0105_dau where logdate=date  group by loghour
    public List<Map> selectDauHourMap(String date);
}
