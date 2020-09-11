package org.liu.process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.liu.apitest.SensorReading

object ProcessDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStreamData = env.socketTextStream("192.168.31.202", 7777)
    val stream = inputStreamData
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })
    //检测每一个传感器温度是否连续上升，10秒之内
    val warningStream = stream
      .keyBy("id")
      .process(new MyProcessTempWarningFunction(10000L))

    warningStream.print("warning")
    env.execute("process1")
  }
}

class MyProcessTempWarningFunction(tine: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Any, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
  //将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //为了删除定时器，需要保存定时器时间戳
  lazy val curTimeStamp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curTime", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimeStamp.value()

    lastTempState.update(value.temp)
    //判断当前温度值，比之前高，并且没有定时器，创建定时器
    if (value.temp > lastTemp && curTimerTs == 0) {
      val ts = ctx.timerService().currentProcessingTime() + tine
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimeStamp.update(ts)
    } else if (value.temp < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      curTimeStamp.clear()
    }
  }

  //定时器触发，10秒内没有下降的数值.报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度连续上升" + tine / 1000 + "秒")
    curTimeStamp.clear()
  }
}
