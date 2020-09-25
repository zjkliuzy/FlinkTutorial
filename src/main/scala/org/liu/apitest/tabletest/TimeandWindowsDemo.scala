package org.liu.apitest.tabletest


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._
import org.liu.apitest.SensorReading
import org.apache.flink.types.Row

object TimeandWindowsDemo {
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
    //    sensor.printSchema()
    //    sensor.toAppendStream[Row].print("row")
    //分组窗口
    //1.table api
    val resultTable = sensor
      .window(Tumble over 10.seconds on 'ts as 'w) //每10秒统计一次
      .groupBy('id, 'w)
      .select('id, 'id.count, 'temp.avg, 'w.end)

    //sql
    tableStream.createTemporaryView("sensor",sensor)
    val resultTableSql = tableStream.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | avg(temp),
        | tumble_end(ts,interval '10' second)
        | from sensor
        | group by
        | id,
        | tumble(ts,interval '10' second)
        |
        |
        |""".stripMargin
    )

    resultTable.toAppendStream[Row].print("result")
    resultTableSql.toRetractStream[Row].print("sql")
    env.execute()
  }

}
