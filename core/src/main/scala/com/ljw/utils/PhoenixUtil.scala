package com.ljw.utils


import com.alibaba.fastjson2.JSONObject
import com.ljw.common.PhoenixConfig

import scala.collection.JavaConversions._
import java.sql.{Connection, SQLException}

object PhoenixUtil {
  /**
   *
   * @param conn
   * @param tablename
   * @param jObject
   * @throws SQLException
   * this exception should be catch in reality execution code, if
   * search logic only depend on Phoenix ,catch this exception will
   * create a difference between dws and ods.
   */
  def upsertValueByJson(conn: Connection,
                        tablename: String, jObject: JSONObject): Unit
  = {
    val columns = jObject.keySet()
    val values = jObject.values()
    val sql = s"upsert into ${PhoenixConfig.HBASE_SCHEMA}.$tablename " +
      s"(${columns.mkString(",")}) values (' ${
        values.mkString("','")} ')"

    val statement = conn.prepareStatement(sql)
    statement.execute()
    conn.commit()

    statement.close()
  }
}
