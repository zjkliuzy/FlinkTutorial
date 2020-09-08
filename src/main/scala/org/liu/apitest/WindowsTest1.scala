package org.liu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowsTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStreamData = env.socketTextStream("192.168.31.202", 7777)
    val value = inputStreamData
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
//      .assignTimestampsAndWatermarks()
    val resultStream = value
      .keyBy("id")
      //  .window(EventTimeSessionWindows.withGap(Time.minutes(1))) //会话窗口
      .timeWindow(Time.seconds(15),Time.seconds(5))
      //      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      //.countWindow(10, 1) //计数窗口
      // .reduce(new MyReduce())//流式
      .apply(new MyWindowsFun())//批处理
    resultStream.print("result")
    env.execute("windows")
  }

}

class MyReduce() extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temp.min(t1.temp))
  }
}

//自定义全窗口函数

class MyWindowsFun() extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
    out.collect((window.getStart, input.size))

  }
}
