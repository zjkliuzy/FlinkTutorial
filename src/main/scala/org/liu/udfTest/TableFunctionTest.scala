package org.liu.udfTest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.liu.apitest.SensorReading
import org.apache.flink.types.Row

object TableFunctionTest {
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
    val split = new Split("_")
    val resultTable = sensor
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    tableStream.createTemporaryView("sensor", sensor)
    tableStream.registerFunction("split", split)
    val resultSql = tableStream.sqlQuery(
      """
        |select
        | id,ts,word,length
        |from
        | sensor, lateral table (split(id)) as splitid(word,length)
        |
        |""".stripMargin
    )
    resultTable.toAppendStream[Row].print("resulr")
    resultSql.toAppendStream[Row].print("resulr")

    env.execute("tableFunction")

  }

}

class Split(septer: String) extends TableFunction[(String, Int)] {
  def eval(str: String) = {
    str.split(septer).foreach(
      word => collect((word, word.length))
    )
  }

}
