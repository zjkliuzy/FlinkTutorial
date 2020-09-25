package org.liu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

object EsOutputTest {
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
    //输出到ES
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("192.168.31.202", 9000, "http")
      .index("sensor")
      .documentType("temp")
    ).inUpsertMode()
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("cnt", DataTypes.BIGINT()))
      .createTemporaryTable("esOutTable")
    aggTable.insertInto("esOutTable")
    env.execute("esoutDemo")

  }
}
