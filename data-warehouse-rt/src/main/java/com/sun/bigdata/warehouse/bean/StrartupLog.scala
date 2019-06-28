package com.sun.bigdata.warehouse.bean

/**
  * title: StrartupLog 
  * projectName data-warehouse-realtime 
  * description: 
  * author Sun-Smile 
  * create 2019-06-26 19:06 
  */
case class StartupLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     ) {

}