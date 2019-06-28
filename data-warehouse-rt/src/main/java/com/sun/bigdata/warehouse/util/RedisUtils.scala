package com.sun.bigdata.warehouse.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * title: RedisUtils 
  * projectName data-warehouse-realtime 
  * description: 获得Redis的连接
  * author Sun-Smile 
  * create 2019-06-26 19:42 
  */
object RedisUtils {
  var jedisPool:JedisPool=null
  def getRedisClient:Jedis={
    // 开辟连接池
    val config: Properties = PropertiesUtil.load("config.properties")
    val host: String = config.getProperty("redis.host")
    val port: Int = config.getProperty("redis.port").toInt

    val jedisConfig = new JedisPoolConfig()
    jedisConfig.setMaxTotal(1024) // 设置最大连接数
    jedisConfig.setMaxIdle(512) // 最大空闲
    jedisConfig.setMinIdle(128) // 最小空闲
    // 设置忙碌时等待
    jedisConfig.setBlockWhenExhausted(true)
    jedisConfig.setMaxWaitMillis(500) // 设置忙碌时的等待时间 500 ms
    // 每次获得连接进行测试
    jedisConfig.setTestOnBorrow(true)

    jedisPool = new JedisPool(jedisConfig , host , port)
    jedisPool.getResource
  }
}
