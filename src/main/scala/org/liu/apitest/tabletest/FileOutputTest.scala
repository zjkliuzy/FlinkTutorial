package org.liu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    val filepath = "F:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filepath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    //转换操作
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor1")

    //聚合转换
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)
    //注册输出表
    val resultpath = "F:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\output.txt"
    tableEnv.connect(new FileSystem().path(resultpath))
        .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        //.field("temp", DataTypes.DOUBLE())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    aggTable.insertInto("outputTable")

//    resultTable.toAppendStream[(String, Double)].print("result")
//    aggTable.toRetractStream[(String, Long)].print("agg")
    env.execute()
  }

}
