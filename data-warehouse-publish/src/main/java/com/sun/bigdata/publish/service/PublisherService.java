package com.sun.bigdata.publish.service;

import java.util.Map;

/**
 * title: PublisherService
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-28 20:14
 */
public interface PublisherService {

    public  Long getDauTotal(String date);

    public Map getDauHour(String date );

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
