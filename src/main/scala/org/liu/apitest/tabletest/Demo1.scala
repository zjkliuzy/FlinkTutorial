package org.liu.apitest.tabletest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.liu.apitest.SensorReading

object Demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.readTextFile("F:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val value = data
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //表执行环境
    val tableEvn = StreamTableEnvironment.create(env)

    //基于流创建一张表
    val table :Table= tableEvn.fromDataStream(value)

    //调用tableApi转换
    val reaultTable = table
        .select("id, temp")
        .filter("id == 'sensor1'")


    tableEvn.createTemporaryView("datatable",table)
    val sql = "select id,temp from datatable where id = 'sensor1'"
    val resultsqlTable = tableEvn.sqlQuery(sql)

    reaultTable.toAppendStream[(String,Double)].print("demo1")
    resultsqlTable.toAppendStream[(String,Double)].print("demo2")
    env.execute("tableTest1")


  }


}
