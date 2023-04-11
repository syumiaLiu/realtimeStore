package com.ljw.app.dim

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DimApp {

  def main(args: Array[String]): Unit = {
    //TODO  1.get execute environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    checkpointConfigInti(env)

    //TODO 2.get data from kafka

    //TODO 3.filter

    //TODO 4.get config data from mysql and broadcast

    //TODO 5.main function

    //TODO 6.sink to HBase by Phoenix

  }
  def checkpointConfigInti(env: StreamExecutionEnvironment): Unit = {
    env enableCheckpointing(300 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(300 * 1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env setRestartStrategy RestartStrategies.fixedDelayRestart(3, 5000L)

    env setStateBackend new HashMapStateBackend()
    env.getCheckpointConfig.setCheckpointStorage("hdfs://ljw:9820/checkpoints")
  }
}
