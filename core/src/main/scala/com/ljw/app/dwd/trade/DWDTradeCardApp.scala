package com.ljw.app.dwd.trade

import com.ljw.utils
import com.ljw.utils.MysqlUtils.getBaseDicLookUpDDL
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils}
import org.apache.flink.table.api.{AnyWithOperations, FieldExpression, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DWDTradeCardApp {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment.create(env)
    //create data table
    tEnv.executeSql("create table topic_db(`database` string, `table` string, `type` string , `data`map<string,string> , `old` map<string,string> , `ts` string, `proc_time` as " +
      "PROCTIME())" + KafkaUtils.createDDLSource("maxwell", "dwdTradeCard"))

    val cartAdd = tEnv.sqlQuery(
      "select data['id']  id , data['user_id'] user_id , data['sku_id'] sku_id , data['source_id'] source_id ,data['source_type'] source_type ," +
        "if(`type`='insert' , data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num, " +
        "ts , proc_time from `topic_db` where `table` = 'cart_info' and (`type` = 'insert' or (`type`= 'update' and `old`['sku_num'] is not null and cast(data['sku_num'] as int) " +
        "> cast(`old`['sku_num'] as int)))"
    )
    tEnv.createTemporaryView("cart_add", cartAdd)

    //load dic table
    tEnv executeSql getBaseDicLookUpDDL

    val resTable = tEnv.sqlQuery("select cadd.id ,user_id , sku_id, source_id, source_type, dic_name source_type_name , sku_num , ts from cart_add cadd join base_dic for " +
      "system_time as of cadd.proc_time" +
      " as dic on cadd.source_type=dic.dic_code ")

    tEnv.createTemporaryView("result_table", resTable)
    //check data by console
//    tEnv.from("result_table").select($"*").execute().print()
    tEnv.executeSql("create table dwd_trade_cart_add  (id string,user_id string,sku_id string, source_id string ,source_type_code string ,source_type_name string, sku_num string" +
      " ,ts string)" + KafkaUtils.createDDLSink("dwd_trade_cart_add"))

    tEnv.executeSql("insert into dwd_trade_cart_add select * from result_table")
  }
}
