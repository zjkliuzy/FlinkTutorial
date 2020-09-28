package org.liu.udfTest

import org.apache.flink.types.Row
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.liu.apitest.SensorReading

object AggFunctionDemo {
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
    //table api
    val avgTemp = new AvgTemp()
    val resultTable = sensor
      .groupBy('id)
      .aggregate(avgTemp('temp) as 'avgTemp)
      .select('id, 'avgTemp)

    tableStream.createTemporaryView("sensor",sensor)
    tableStream.registerFunction("avgTemp",avgTemp)
    val resultSql = tableStream.sqlQuery(
      """
        |select
        | id,avgTemp(temp)
        | from
        | sensor
        | group by id
        |""".stripMargin
    )
    resultTable.toRetractStream[Row].print("result")
  //  resultSql.toRetractStream[Row].print("sql")
    env.execute("agg")

  }

}

//定义一个类专门表示聚合的状态
class AvgTempAcc() {
  var sum = 0.0
  var count = 0
}

//自定义一个聚合函数,每个sensor的平均温度
class AvgTemp() extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  //实现一个具体计算函数
  def accumulate(accumulator: AvgTempAcc, temp: Double) = {
    accumulator.sum += temp
    accumulator.count += 1
  }
}
