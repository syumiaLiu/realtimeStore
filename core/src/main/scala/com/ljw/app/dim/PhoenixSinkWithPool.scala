//package com.ljw.app.dim
//
//import com.alibaba.fastjson2.JSONObject
//import com.ljw.common
//import com.ljw.common.PhoenixConfig
//import com.ljw.utils.PhoenixUtil
//import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//
///**
// * phoenix doc advice use connection object not a pool
// */
//class PhoenixSinkWithPool extends RichSinkFunction[JSONObject]{
//  var ds: HikariDataSource = _
//  override def open(parameters: Configuration): Unit = {
//    val config = new HikariConfig()
//    config setDriverClassName PhoenixConfig.PHOENIX_DRIVER
//    config setJdbcUrl PhoenixConfig.PHOENIX_SERVER
//    config.addDataSourceProperty("cachePrepStmts", "true")
//    config.addDataSourceProperty("prepStmtCacheSize", "250")
//    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
//    config setMaximumPoolSize 20
//    ds = new HikariDataSource(config)
//  }
//
//  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
//    val conn = ds.getConnection
//    val tableName = value getString "sinkTable"
//    val data = value getJSONObject "data"
//    PhoenixUtil.upsertValueByJson(conn,tableName,data)
//
//    conn.close()
//  }
//
//
//}
