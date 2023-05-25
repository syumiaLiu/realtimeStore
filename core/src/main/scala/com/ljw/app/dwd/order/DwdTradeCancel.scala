package com.ljw.app.dwd.order

import com.ljw.app.dwd.{DWDConfig, DWDTableSchema}
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object DwdTradeCancel {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofMinutes(30))

    tEnv.executeSql("create table dwd_order_detail_proc(" +
      s"${DWDTableSchema.DWD_ORDER_DETAIL_PROC_SCHEMA})" +
      s"${KafkaUtils.createDDLSource("dwd_order_detail_proc", "dwdcancel")}")
    val cancelTable = tEnv.sqlQuery("select " +
      DWDTableSchema.DWD_ORDER_DETAIL_SELECT_BASE +
      "from dwd_order_detail_proc " +
      "where `type` = 'update' " +
      s"and order_status = '${DWDConfig.CANCEL_SOURCE_ID}'")
    tEnv.createTemporaryView("cancelTable", cancelTable)

    tEnv.executeSql("create table dwd_trade_cancel_detail" +
      s"(${DWDTableSchema.DWD_ORDER_SPLIT_TABLE_OUTPUT_SCEHMA})" +
      KafkaUtils.createDDLSink("dwd_trade_cancel_detail")
    )
    tEnv.executeSql("insert into dwd_trade_cancel_detail select * from cancelTable")
  }

}
