package org.liu.udfTest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector
import org.liu.apitest.SensorReading
import org.apache.flink.types.Row
object TableAggFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    val seeting: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inStreamingMode()
    //      .build()


    val tableStream = StreamTableEnvironment.create(env)
    val inputStreamData = env.readTextFile("F:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val value = inputStreamData
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
    val sensor = tableStream.fromDataStream(value, 'id, 'timestamp.rowtime as 'ts, 'temp)
    val top2Temp = new Top2Temp
    val resultTable = sensor
      .groupBy('id)
      .flatAggregate(top2Temp('temp) as('temp, 'rank))
      .select('id, 'temp, 'rank)
    resultTable.toRetractStream[Row].print("result")
    env.execute("tableAgg")

  }

}

class TempAcc {
  var highest: Double = Double.MinValue
  var secondTemp: Double = Double.MinValue
}

//自定义聚合函数
//提取最高的两个温度
class Top2Temp extends TableAggregateFunction[(Double, Int), TempAcc] {
  override def createAccumulator(): TempAcc = new TempAcc

  //实现计算聚合结果的函数
  def accumulate(acc: TempAcc, temp: Double): Unit = {
    if (temp > acc.highest) {
      acc.secondTemp = acc.highest
      acc.highest = temp
    } else if (temp > acc.secondTemp) {
      acc.secondTemp = temp
    }
  }

  //实现一个输出结果的方法，最终处理完所有数据时调用
  def emitValue(acc: TempAcc, out: Collector[(Double, Int)]) = {
    out.collect((acc.highest, 1))
    out.collect((acc.secondTemp, 2))
  }

}
