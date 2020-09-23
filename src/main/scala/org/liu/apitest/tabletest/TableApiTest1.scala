package org.liu.apitest.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

object TableApiTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

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
    val blinkBatchEnv = TableEnvironment.create(blinkbatchSetting)
  }
}
