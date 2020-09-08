package org.liu.apitest.Sinkdemo

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.liu.apitest.{MySensorSource, SensorReading}

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.addSource(new MySensorSource())
    val value = inputStream
//      .map(d => {
//        val dataArrray = d.split(",")
//        SensorReading(dataArrray(0), dataArrray(1).toLong, dataArrray(2).toDouble)
//      })
    value.addSink(new MyJdbcSink())
    env.execute("jdbc sink")
  }

  //自定义一个sinkfunction
  class MyJdbcSink() extends RichSinkFunction[SensorReading] {
    //定义sql连接
    //定义预编译sql
    var conn: Connection = _
    var insertSta: PreparedStatement= _
    var updateSta: PreparedStatement = _

    //在open生命周期中创建连接和语句
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.31.201:3306/test?serverTimezone=GMT%2B8", "root", "123")
      insertSta = conn.prepareStatement("insert into temp (sensor,tempu) value (?,?)")
      updateSta = conn.prepareStatement("update temp set tempu = ? where sensor = ? ")
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      //执行更新语句
      updateSta.setDouble(1,value.temp)
      updateSta.setString(2,value.id)
      updateSta.execute()
      if (updateSta.getUpdateCount==0){
        insertSta.setString(1,value.id)
        insertSta.setDouble(2,value.temp)
        insertSta.execute()
      }
    }
    //关闭
    override def close(): Unit = {
      insertSta.close()
      updateSta.close()
      conn.close()
    }
  }

}
