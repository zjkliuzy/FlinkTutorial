package org.liu.apitest.Sinkdemo

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.liu.apitest.SensorReading

object EsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val value = inputStream
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //定义httpHost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.21.202", 9200))
    //定义ElasticSearchSingFunction
    val esSinkFun = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        //包装写入es的数据
        val data = new util.HashMap[String, String]()
        data.put("id", t.id)
        data.put("temp", t.temp.toString)
        data.put("time", t.timestamp.toString)
        //indexer发送数据
        //创建index request
        val indexRequest = Requests.indexRequest().index("sensor")
          .`type`("readingdata")
          .source(data)
        indexer.add(indexRequest)
        println(t + "保存")
      }

    }
    val newSink = new ElasticsearchSink.Builder[SensorReading](httpHosts,esSinkFun).build()
   // value.addSink(newSink)

    env.execute("sink3")
  }
}
