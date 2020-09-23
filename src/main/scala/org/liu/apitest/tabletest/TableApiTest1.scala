package org.liu.apitest.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

object TableApiTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    /*
        //1.1 老版本的planner流处理
        val setting = EnvironmentSettings.newInstance()
          .useOldPlanner()
          .inStreamingMode()
          .build()
        val oldStreamTableEnv = StreamTableEnvironment.create(env,setting)

        //1.2老版本的planner批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)
        //1.3 新版本planner流处理
        val blinkStramSetting = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val blinkStreamEnv = StreamTableEnvironment.create(env,blinkStramSetting)

        //1.4 新版本批处理
        val blinkbatchSetting = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inBatchMode()
          .build()
        val blinkBatchEnv = TableEnvironment.create(blinkbatchSetting)*/
    val filepath = "F:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filepath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "192.168.31.202:2081")
      .property("bootstrap.server", "192.168.31.202:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkainputTable")

    //3 查询转换
    //tableApi
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor1")

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temp
        |from inputTable
        |where id = 'sensor1'
        |""".stripMargin
    )


//    val inputTable = tableEnv.from("inputTable")
    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("resultSql")
    env.execute("tableAPITest1")

  }
}
