package com.ljw.utils

import com.ljw.common.MysqlConfig._

object MysqlUtils {
  def getBaseDicLookUpDDL = {
    "create table `base_dic`(\n" +
      "`dic_code` string,\n" +
      "`dic_name` string,\n" +
      "`parent_code` string,\n" +
      "`create_time` timestamp,\n" +
      "`operate_time` timestamp,\n" +
      "primary key(`dic_code`) not enforced\n" +
      ")" + mysqlLookUpTableDDL("base_dic")
  }

  def mysqlLookUpTableDDL(tableName: String) = {
    "WITH(\n" +
    "'connector' = 'jdbc',\n" +
      s"'url' = 'jdbc:mysql://$host:$port/gmall',\n" +
      "'table-name' = '" + tableName + "',\n" +
      "'lookup.cache.max-rows' = '10',\n" +
      "'lookup.cache.ttl' = '1 hour',\n" +
      s"'username' = '$username',\n" +
      s"'password' = '$password',\n" +
      "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
      ")"
  }
}
