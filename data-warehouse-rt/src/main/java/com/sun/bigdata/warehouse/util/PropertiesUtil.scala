package com.sun.bigdata.warehouse.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * title: PropertiesUtil 
  * projectName data-warehouse-realtime 
  * description: 返回加载文件的 Properties
  * author Sun-Smile 
  * create 2019-06-26 19:32 
  */
object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    properties
  }
}
