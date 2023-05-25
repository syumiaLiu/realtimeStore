package com.ljw.app.dwd.order

import com.ljw.utils.FlinkEnvUtil
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object DwdTradePayDetail {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofMinutes(30))


  }
}
