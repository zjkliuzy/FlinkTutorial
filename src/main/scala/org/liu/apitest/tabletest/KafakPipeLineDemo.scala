package org.liu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafakPipeLineDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

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

    val sensorTable = tableEnv.from("kafkainputTable")
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor1")

    //聚合转换
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)


    //输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .property("zookeeper.connect", "192.168.31.202:2081")
      .property("bootstrap.server", "192.168.31.202:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")

    env.execute("kafkapipeTest")
  }

}
