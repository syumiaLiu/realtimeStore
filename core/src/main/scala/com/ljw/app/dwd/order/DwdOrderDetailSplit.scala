package com.ljw.app.dwd.order

import com.ljw.app.dwd.DWDTableSchema
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object DwdOrderDetailSplit {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofMinutes(30))

    val utils = new KafkaUtils

    tEnv.executeSql("create table dwd_order_detail_proc(" +
      s"${DWDTableSchema.DWD_ORDER_DETAIL_PROC_SCHEMA}" +
      " ,primary key(id) not enforced)" +
      s"${utils.createDDLSource("dwd_order_detail_proc" ,"dwdOrderSplit")}")

  }

}
