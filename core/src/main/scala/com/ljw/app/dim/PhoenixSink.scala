package com.ljw.app.dim

import com.alibaba.fastjson2.JSONObject
import com.ljw.common.PhoenixConfig
import com.ljw.utils.PhoenixUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager}

class PhoenixSink extends RichSinkFunction[JSONObject]{
  override def open(parameters: Configuration): Unit = {
    Class.forName(PhoenixConfig.PHOENIX_DRIVER)
  }

  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    val connection = DriverManager.getConnection(PhoenixConfig
      .PHOENIX_SERVER)
    val tableName = value getString "sinkTable"
    val data = value getJSONObject "data"
    PhoenixUtil.upsertValueByJson(connection, tableName, data)

    connection.close()
  }
}
