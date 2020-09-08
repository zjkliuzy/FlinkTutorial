package org.liu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamingWordcount {

  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从配置里面读取host和port
    val para = ParameterTool.fromArgs(args)
    //socket文本流
    val inputStreamData = env.socketTextStream("192.168.31.202", 7777)

    val resultStream: DataStream[(String, Int)] = inputStreamData
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1)) //转换成二元组
      .keyBy(0)
      .sum(1)

    resultStream.print().setParallelism(1)
    env.execute() //启动处理逻辑


  }
}
