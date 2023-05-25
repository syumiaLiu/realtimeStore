package com.ljw.app.dim

import com.ljw.bean.TableProcessConfig
import com.ljw.utils.FlinkEnvUtil.{checkpointConfigInti, createLocalEnv}
import com.ljw.utils.KafkaUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DimApp {

  def main(args: Array[String]): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env =  createLocalEnv()
//    env.enableCheckpointing(3000)
    //only for test,cdc will read binlog after a success checkpoint.
    checkpointConfigInti(env)
    val input = env.fromSource(KafkaUtils.createDataStream("maxwell",
      "dim"),
      WatermarkStrategy.noWatermarks(), "topic_db")


    val inValidOutputTag = new OutputTag[String]("invalidData")
    val filter = input.process(new JsonFilterFunction)
    val inValidOutput = filter.getSideOutput(inValidOutputTag)
    val configStream = env.fromSource(MysqlSourceFunction
      .createMysqlSource(
        "gmall_config", "gmall_config.table_process"),
      WatermarkStrategy.noWatermarks(),
      "mysql_config_source")
      .setParallelism(1)
    val configMapStateDesc = new MapStateDescriptor[String,
      TableProcessConfig]("dim-config", classOf[String], classOf[TableProcessConfig])
    val broadConfigStream = configStream
      .broadcast(configMapStateDesc)
    val dimDS = filter.connect(broadConfigStream)
      .process(new ConfigBroadProcFunc)

    dimDS.addSink(new PhoenixSink)
    env.execute("dim filter")
  }

}
