package com.ljw.app.dwd.order

import com.ljw.utils.{FlinkEnvUtil, KafkaUtils, MysqlUtils}
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object DwdTradeOrderRefund {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(30))

    tEnv.executeSql("create table topic_db(`database` string, `table` string, `type` string , `data`map<string,string> , `old` map<string,string> , `ts` string, `proc_time` as " +
      "PROCTIME())" + KafkaUtils.createDDLSource("maxwell", "dwdTradeOrderRefund"))

    val order_refund_info = tEnv.sqlQuery("select " +
      "data['id'] id, " +
      "data['user_id']  user_id, " +
      "data['order_id']  order_id, " +
      "data['sku_id']  sku_id, " +
      "data['refund_type']  refund_type, " +
      "data['refund_num']  refund_num, " +
      "data['refund_amount']  refund_amount, " +
      "data['refund_reason_type']  refund_reason_type, " +
      "data['refund_reason_txt']  refund_reason_txt, " +
      "data['refund_status']  refund_status, " +
      "data['create_time'] create_time, " +
      "proc_time, ts " +
      "from topic_db "  +
      "where `table` = 'order_refund_info' " +
      "and `type` = 'insert'"
    )

    tEnv.createTemporaryView("order_refund_info",order_refund_info)

    val order_info = tEnv.sqlQuery("select " +
      "data['id']  id , " +
      "data['province_id'] province_id ," +
      "`old` " +
      "from topic_db " +
      "where `table` = 'order_info' " +
      "and `type` = 'update' " +
      "and data['order_status'] = '1005' " +
      "and `old`['order_status'] is not null")


    tEnv.createTemporaryView("order_info" ,order_info)


    tEnv.executeSql(MysqlUtils.getBaseDicLookUpDDL)

    val resTable = tEnv.sqlQuery("select " +
      "ori.id id," +
      "ori.user_id user_id," +
      "ori.order_id order_id," +
      "ori.sku_id sku_id," +
      "ori.refund_type refund_type," +
      "ori.refund_num refund_num," +
      "ori.refund_amount refund_amount," +
      "ori.refund_reason_type refund_reason_type," +
      "ori.refund_reason_txt refund_reason_txt," +
      "ori.refund_status refund_status," +
      "ori.create_time create_time," +
      "oi.province_id province_id," +
      "dic.dic_code order_status " +
      "from order_refund_info  ori " +
      "join order_info oi " +
      "on ori.order_id = oi.id " +
      "join base_dic " +
      "for system_time as of ori.proc_time  " +
      "as dic " +
      "on ori.refund_type = dic.dic_code")

    tEnv.createTemporaryView("resTable" , resTable)
    tEnv.executeSql("create table dwd_trade_order_refund(" +
      "id string," +
      "user_id string," +
      "order_id string," +
      "sku_id string," +
      "refund_type string," +
      "refund_num string," +
      "refund_amount string," +
      "refund_reason_type string," +
      "refund_reason_txt string," +
      "refund_status string," +
      "create_time string," +
      "province_id string," +
      "order_status string)" +
      s"${KafkaUtils.createDDLSink("dwd_trade_order_refund")}")
    tEnv.executeSql("insert into dwd_trade_order_refund select * from resTable")
  }

}
