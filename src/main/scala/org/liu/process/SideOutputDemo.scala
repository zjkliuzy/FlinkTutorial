package org.liu.process

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.liu.apitest.SensorReading

object SideOutputDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStreamData = env.socketTextStream("192.168.31.202", 7777)
    val value = inputStreamData
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //用processfunction的侧输出流实现分流操作
    val highTempStream = value
      .process(new SplitTempProcesser(30))

    val lowTempStream = highTempStream
      .getSideOutput(new OutputTag[(String, Double, Long)]("lowTemp"))
    highTempStream.print("high")
    lowTempStream.print("low")
    env.execute("sideOutputDemo")

  }

}

class SplitTempProcesser(d: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //判断数据文档值，大于value输出到主流，小于输出到sideoutput
    if (value.temp > d) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[(String, Double, Long)]("lowTemp"), (value.id, value.temp, value.timestamp))
    }
  }
}
