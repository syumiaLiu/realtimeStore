package com.ljw.utils

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkEnvUtil {
    def createLocalEnv(): StreamExecutionEnvironment = {
      createLocalEnv(useCheckpoint = false,useFSBackend = false)
    }

    def createLocalEnv(useCheckpoint: Boolean , useFSBackend: Boolean): StreamExecutionEnvironment = {
      val conf = new Configuration
      conf setString (RestOptions.BIND_PORT, "9091-10000")
      val env = StreamExecutionEnvironment
        .createLocalEnvironmentWithWebUI(conf)
      if(useCheckpoint) checkpointConfigInti(env)
      if(useFSBackend) useHDFSBackend(env)
      env
    }

  /**
   * default use memory
   */
  def checkpointConfigInti(env: StreamExecutionEnvironment): Unit = {
    env enableCheckpointing(300 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(300 * 1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env setRestartStrategy RestartStrategies.fixedDelayRestart(3, 5000L)


  }

  def useHDFSBackend(environment: StreamExecutionEnvironment): Unit = {
    environment setStateBackend new HashMapStateBackend()
    environment.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:9820/checkpoints")
    System setProperty ("HADOOP_USER_NAME" ,"ljw")
  }

}
