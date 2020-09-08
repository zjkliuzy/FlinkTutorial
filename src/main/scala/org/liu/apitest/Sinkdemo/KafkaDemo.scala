package org.liu.apitest.Sinkdemo

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.liu.apitest.SensorReading

object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //val inputStream = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val properties = new Properties()

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val dataStream = inputStream
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble).toString
      })
    //    dataStream.addSink(StreamingFileSink.forRowFormat(
    //      new Path("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\out.txt"),
    //      new SimpleStringEncoder[String]("UTF-8")
    //    ).build())//文件

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092","test",new SimpleStringSchema()))//kafka
    env.execute("sink1")
  }
}
