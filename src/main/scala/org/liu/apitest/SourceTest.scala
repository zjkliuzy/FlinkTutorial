package org.liu.apitest


import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temp: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor1", 1547718199, 55.5),
      SensorReading("sensor2", 1547719099, 5.6),
      SensorReading("sensor3", 1547729799, 78.6),
      SensorReading("sensor4", 1547740199, 35.6)
    ))
    //    env.fromElements(0, 1.4, 323, "dsds")

    stream1.print("stream1")
    env.execute("job1")
    val value: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //从文件输入
    val stream2 = value

    stream2.print("stream2")
    env.execute("job2")
    //socket文本流
    //val stream3 = env.socketTextStream("localhost",7777)

    //从kafka读取
    val properties = new Properties()

    // val stream4 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    //自定义source
    val stream5 = env.addSource(new MySensorSource())
    stream5.print("stream5")
    env.execute("job5")
  }
}

//实现自定义的sourceFunction,自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据源是否正常运行
  var running = true

  //随机生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor" + i, 60 + rand.nextGaussian() * 20)

    )
    while (running) {
      curTemp = curTemp.map(
        d => (d._1, d._2 + rand.nextGaussian())
      )
      //获取当前·时间
      val curTime = System.currentTimeMillis()
      //包装成样例类,
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2)

        ))
      //间隔时间
      Thread.sleep(1000)


    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

