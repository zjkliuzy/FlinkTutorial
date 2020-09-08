package org.liu.apitest

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object TransTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1.基本转换操作
    val data = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = data
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //2.滚动聚合操作
    val aggStream = dataStream
      .keyBy("id")
      //  .keyBy(dd=>dd.id)
      //.minBy("temp")
      .reduce((curRes, newData) => {
        SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temp.min(newData.temp))
      }) //聚合出每个sensor的最大时间和最小温度

    //分流操作
    val splitStream = dataStream
      .split(data => {
        if (data.temp > 20) {
          Seq("high")
        } else {
          Seq("low")
        }
      })
    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")

    val warnStream = highStream.map(
      data => (data.id, data.temp)
    )
    val value: ConnectedStreams[(String, Double), SensorReading] = warnStream.connect(lowStream)
    val connectedStream = value
    val resultData = connectedStream.map(
      warnData => (warnData._1, warnData._2, "warn"),
      lowData => (lowData.id, "normal")
    )
    resultData.print("res")

    //aggStream.print()
    //    highStream.print("high")
    //    lowStream.print("low")
    env.execute("jobTrans")
  }
  //自定义mapfunction
//  class MyMapFunction extends MapFunction{
//
//  }
}
