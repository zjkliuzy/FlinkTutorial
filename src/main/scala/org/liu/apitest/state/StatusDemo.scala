package org.liu.apitest.state

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.liu.apitest.SensorReading

object StatusDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    /*    env.setStateBackend( new FsStateBackend(""))
        env.setStateBackend(new RocksDBStateBackend("",true))配置状态后端*/
    val inputStreamData = env.socketTextStream("192.168.31.202", 7777)
    val value = inputStreamData
      .map(d => {
        val dataArrray = d.split(",")
        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
      })


    env.execute("statusJob")
  }
}

class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {
  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("listState", classOf[String]))
  var myStatus: ValueState[Int] = _
  lazy val myMapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("map-state", classOf[String], classOf[Double]))
  lazy val myReduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduce-state", new ReduceFunction[SensorReading] {
    override def reduce(t: SensorReading, t1: SensorReading): SensorReading = SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temp.min(t1.temp))
  }, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    myStatus = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my_state", classOf[Int]))
  }

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, Int]#Context, collector: Collector[Int]): Unit = {
    myStatus.value()
    myStatus.update(1)
    myListState.add("haha")
    myReduceState.add(i)

  }
}