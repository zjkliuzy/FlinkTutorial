package org.liu.udfTest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.liu.apitest.SensorReading
import org.apache.flink.types.Row
object ScalarFunctionTest {
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
    //调用自定义hash函数，对id进线hash运算
    //table api
    val hashCode = new HashCode(11)
    val resultTable = sensor
      .select('id,'ts,hashCode('id))

    //sql
    //需要在表环境里面注册
    tableStream.createTemporaryView("sensor",sensor)
    tableStream.registerFunction("hasCode",hashCode)
    val resultSqlTable = tableStream.sqlQuery(
      "select id,ts,hasCode(id) from sensor"
    )
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")
    env.execute("scalar")
  }

}
//自定义标狼函数
class HashCode(factor:Int)extends ScalarFunction{
  def eval (s:String):Int = {
    s.hashCode * factor -10000
  }

}
