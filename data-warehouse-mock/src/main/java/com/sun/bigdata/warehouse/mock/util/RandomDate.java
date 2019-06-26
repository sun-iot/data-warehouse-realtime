package com.sun.bigdata.warehouse.mock.util;

import java.util.Date;
import java.util.Random;

/**
 * title: RandomDate
 * projectName data-warehouse-realtime
 * description:
 * author Sun-Smile
 * create 2019-06-25 17:47
 */
public class RandomDate {
    Long logDateTime =0L;//
    int maxTimeStep=0 ;


    public RandomDate (Date startDate , Date  endDate, int num) {

        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();

    }


    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }
}
