package org.liu.apitest.Sinkdemo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.liu.apitest.SensorReading

object RedisTesst {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val value = inputStream
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //定义redis配置类
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.31.202")
      .setPort(6379)
      .build()
    //定义redisMapper
    val mapper = new RedisMapper[SensorReading] {
      //定义保存到redis的命令，hset table_name key value
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }

      override def getKeyFromData(t: SensorReading): String = {
        t.temp.toString
      }

      override def getValueFromData(t: SensorReading): String = t.id
    }

    value.addSink(new RedisSink[SensorReading](conf, mapper))
    env.execute("sink2")
  }

}
