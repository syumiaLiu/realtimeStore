package com.ljw.app.dwd.order

import com.ljw.app.dwd.DWDTableSchema
import com.ljw.utils.MysqlUtils.getBaseDicLookUpDDL
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration

object DwdTradeOrderPreProcess {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment create env
    tEnv.getConfig.setIdleStateRetention(Duration.ofMinutes(30))

    //create data table
    tEnv.executeSql(s"create table topic_db(${DWDTableSchema.DWD_INPUT_SCHEMA_WIHT_TIME})" + KafkaUtils.createDDLSource("maxwell", "orderPreProc"))

    //order-detail
    val order_detail = tEnv.sqlQuery("select " +
      "data['id'] id ," +
      "data['order_id'] order_id, " +
      "data['sku_id'] sku_id," +
      "data['sku_name'] sku_name," +
      "data['img_url'] img_url," +
      "data['order_price'] order_price, " +
      "data['sku_num'] sku_num, " +
      "data['create_time'] create_time, " +
      "data['source_type'] source_type, " +
      "data['source_id'] source_id, " +
      "data['split_total_amount'] split_total_amount, " +
      "data['split_activity_amount'] split_activity_amount, " +
      "data['split_coupon_amount']  split_coupon_amount, " +
      "proc_time " +
      "from `topic_db` " +
      "where `database` = 'gmall'" +
      " and `table` = 'order_detail'" +
      " and `type` = 'insert'")
    tEnv.createTemporaryView("order_detail", order_detail)

    //order_info
    val order_info = tEnv.sqlQuery("select " +
      "data['id']   id," +
      "data['consignee']   consignee," +
      "data['consignee_tel']   consignee_tel," +
      "data['total_amount']   total_amount," +
      "data['order_status']   order_status," +
      "data['user_id']   user_id," +
      "data['payment_way']   payment_way," +
      "data['delivery_address']   delivery_address," +
      "data['order_comment']   order_comment," +
      "data['out_trade_no']   out_trade_no," +
      "data['trade_body']   trade_body," +
      "data['create_time']   create_time," +
      "data['operate_time']   operate_time," +
      "data['expire_time']   expire_time," +
      "data['process_status']   process_status," +
      "data['tracking_no']   tracking_no," +
      "data['parent_order_id']   parent_order_id," +
      "data['img_url']   img_url," +
      "data['province_id']   province_id," +
      "data['activity_reduce_amount']  activity_reduce_amount, " +
      "data['coupon_reduce_amount']   coupon_reduce_amount," +
      "data['original_total_amount']  original_total_amount," +
      "data['feight_fee']   feight_fee," +
      "data['feight_fee_reduce']   feight_fee_reduce," +
      "data['refundable_time']   refundable_time," +
      "proc_time ," +
      "`type`," +
      "`old` " +
      "from `topic_db` " +
      "where `database` = 'gmall' " +
      "and `table` = 'order_info' " +
      "and (`type` = 'insert'  or `type` = 'update') "
    )
    tEnv.createTemporaryView("order_info", order_info)


    //order_detail_activity
    val order_detail_activity = tEnv.sqlQuery("select " +
      "    data['id']  id," +
      "    data['order_id']  order_id," +
      "    data['order_detail_id']  order_detail_id," +
      "    data['activity_id']  activity_id, " +
      "    data['activity_rule_id']  activity_rule_id," +
      "    data['sku_id']  sku_id," +
      "    data['create_time']  create_time " +
      "    from `topic_db` " +
      "where `database` = 'gmall' " +
      "and `table` = 'order_detail_activity' " +
      "and `type` = 'insert'")
    tEnv.createTemporaryView("order_detail_activity" , order_detail_activity)

    //order_detail_coupon
    val order_detail_coupon = tEnv.sqlQuery("select " +
      "  data['id'] id," +
      "  data['order_id']  order_id," +
      "  data['order_detail_id']  order_detail_id," +
      "  data['coupon_id']  coupon_id," +
      "  data['coupon_use_id']  coupon_use_id," +
      "  data['sku_id']  sku_id," +
      "  data['create_time']  create_time" +
      "  from topic_db " +
      "where `database` = 'gmall' " +
      "and `table` = 'order_detail_coupon' " +
      "and `type` = 'insert' ")
    tEnv.createTemporaryView("order_detail_coupon" , order_detail_coupon)

    //load dic table
    tEnv executeSql getBaseDicLookUpDDL
    //join
    val resTable = tEnv.sqlQuery("select " +
      "    od.id," +
      "    od.order_id," +
      "    od.sku_id," +
      "    od.sku_name," +
      "    od.img_url," +
      "    od.order_price," +
      "    od.sku_num," +
      "    od.create_time," +
      "    od.source_type," +
      "    od.source_id," +
      "    od.split_total_amount," +
      "    od.split_activity_amount," +
      "    od.split_coupon_amount," +
      "    oi.consignee," +
      "    oi.consignee_tel," +
      "    oi.total_amount," +
      "    oi.order_status," +
      "    oi.user_id," +
      "    oi.payment_way," +
      "    oi.delivery_address," +
      "    oi.order_comment," +
      "    oi.out_trade_no," +
      "    oi.trade_body," +
      "    oi.operate_time," +
      "    oi.expire_time," +
      "    oi.process_status," +
      "    oi.tracking_no," +
      "    oi.parent_order_id," +
      "    oi.province_id," +
      "    oi.activity_reduce_amount," +
      "    oi.coupon_reduce_amount," +
      "    oi.original_total_amount," +
      "    oi.feight_fee," +
      "    oi.feight_fee_reduce," +
      "    oi.refundable_time," +
      "    oa.id order_detail_activity_id," +
      "    oa.activity_id," +
      "    oa.activity_rule_id," +
      "    oc.id order_detail_coupon_id," +
      "    oc.coupon_id," +
      "    oc.coupon_use_id," +
      "    oi.`type` ," +
      "    oi.`old`  ," +
      "    dic.dic_name  source_type_name," +
      "    dic1.dic_name order_status_name " +
      "from order_detail od " +
      "join order_info oi " +
      "on od.order_id = oi.id " +
      "left join order_detail_activity oa " +
      "on od.order_id = oa.order_detail_id " +
      "left join " +
      "order_detail_coupon oc " +
      "on od.order_id = oc.order_detail_id " +
      "join base_dic for system_time as of od.proc_time as dic " +
      "on od.source_type = dic.dic_code " +
      "join base_dic for " +
      "system_time as of oi.proc_time as dic1 " +
      "on oi.order_status = dic1.dic_code")
    tEnv.createTemporaryView("resTable" , resTable)

    //create upsert sink
      tEnv.executeSql("create table dwd_order_detail_proc(" +
        DWDTableSchema.DWD_ORDER_DETAIL_PROC_SCHEMA +
       "," +
        "primary key(id) not enforced)" +
        s"${KafkaUtils.createUpsertDDLSink("dwd_order_detail_proc")}")

        tEnv.executeSql("insert into dwd_order_detail_proc select * from resTable")
    //table data test
//        tEnv.toRetractStream[Row](resTable)
//          .print()
//        env.execute("table data test")
  }

}
