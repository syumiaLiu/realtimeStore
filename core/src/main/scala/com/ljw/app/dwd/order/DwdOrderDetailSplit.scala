package com.ljw.app.dwd.order

import com.ljw.app.dwd.{DWDConfig, DWDTableSchema}
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object DwdOrderDetailSplit {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofMinutes(30))


    tEnv.executeSql("create table dwd_order_detail_proc(" +
      s"${DWDTableSchema.DWD_ORDER_DETAIL_PROC_SCHEMA})" +
      s"${KafkaUtils.createDDLSource("dwd_order_detail_proc", "dwdOrderSplit")}")
    val placeOrderTable = tEnv.sqlQuery("select " +
                            DWDTableSchema.DWD_ORDER_DETAIL_SELECT_BASE +
                            "from dwd_order_detail_proc " +
                            "where `type` = 'insert'")
    tEnv.createTemporaryView("placeOrderTable" , placeOrderTable)

    tEnv.executeSql("create table dwd_trade_order_detail" +
      s"(${DWDTableSchema.DWD_ORDER_SPLIT_TABLE_OUTPUT_SCEHMA})" +
      KafkaUtils.createDDLSink("dwd_trade_order_detail")
      )

    tEnv.executeSql("insert into dwd_trade_order_detail select * from placeOrderTable")


  }

}
