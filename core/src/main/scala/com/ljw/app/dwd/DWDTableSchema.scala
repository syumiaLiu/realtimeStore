package com.ljw.app.dwd

object DWDTableSchema {
  val DWD_ORDER_DETAIL_PROC_SCHEMA =
    "  `id` string," +
    "  `order_id` string," +
    "  `sku_id` string," +
    "  `sku_name` string," +
    "  `img_url` string," +
    "  `order_price` string," +
    "  `sku_num` string," +
    "  `create_time` string," +
    "  `source_type` string," +
    "  `source_id` string," +
    "  `split_total_amount` string," +
    "  `split_activity_amount` string," +
    "  `split_coupon_amount` string," +
    "  `consignee` string," +
    "  `consignee_tel` string," +
    "  `total_amount` string," +
    "  `order_status` string," +
    "  `user_id` string," +
    "  `payment_way` string," +
    "  `delivery_address` string," +
    "  `order_comment` string," +
    "  `out_trade_no` string," +
    "  `trade_body` string," +
    "  `operate_time` string," +
    "  `expire_time` string," +
    "  `process_status` string," +
    "  `tracking_no` string," +
    "  `parent_order_id` string," +
    "  `province_id` string," +
    "  `activity_reduce_amount` string," +
    "  `coupon_reduce_amount` string," +
    "  `original_total_amount` string," +
    "  `feight_fee` string," +
    "  `feight_fee_reduce` string," +
    "  `refundable_time` string," +
    "  `order_detail_activity_id` string," +
    "  `activity_id` string," +
    "  `activity_rule_id` string," +
    "  `order_detail_coupon_id` string," +
    "  `coupon_id` string," +
    "  `coupon_use_id` string," +
    "  `type` string," +
    "  `old` map<string,string>," +
    "  `source_type_name` string," +
    "  `order_status_name` string "

  val DWD_INPUT_SCHEME = "`database` string, " +
    "`table` string, " +
    "`type` string , " +
    "`data`map<string,string> , " +
    "`old` map<string,string> , " +
    "`ts` string "
  val DWD_INPUT_SCHEMA_WIHT_TIME = DWD_INPUT_SCHEME +
    ", " +
    "`proc_time` as PROCTIME()"

  val DWD_ORDER_SPLIT_TABLE_OUTPUT_SCEHMA = "id  string," +
    "order_id  string," +
    "user_id  string," +
    "sku_id  string," +
    "sku_name  string," +
    "sku_num  string, " +
    "order_price  string , " +
    "province_id  string," +
    "activity_id  string," +
    "activity_rule_id  string," +
    "coupon_id  string," +
    "create_time  string," +
    "source_id  string," +
    "source_type_id  string," +
    "source_type_name  string," +
    "split_activity_amount  string," +
    "split_coupon_amount  string," +
    "split_total_amount  string "


  val DWD_ORDER_DETAIL_SELECT_BASE =
    "id," +
    "order_id," +
    "user_id," +
    "sku_id," +
    "sku_name," +
    "sku_num, " +
    "order_price , " +
    "province_id," +
    "activity_id," +
    "activity_rule_id," +
    "coupon_id," +
    "create_time," +
    "source_id," +
    "source_type source_type_id," +
    "source_type_name," +
    "split_activity_amount," +
    "split_coupon_amount," +
    "split_total_amount "
}
