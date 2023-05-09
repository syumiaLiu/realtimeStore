package com.ljw.app.dim

import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.ljw.common.MysqlConfig._

object MysqlSourceFunction {

    val table = "test"
    def createMysqlSource(host: String, port: Int,
                          username: String , password: String ,
                          database:String , table: String)
    :MySqlSource[String] = {
        MySqlSource.builder[String]()
          .hostname(host)
          .port(port)
          .username(username)
          .password(password)
          .databaseList(database)
          .tableList(table)
          .startupOptions(StartupOptions.initial())
          .deserializer(new JsonDebeziumDeserializationSchema())
          .build()
    }

    def createMysqlSource(database: String, table: String)
    : MySqlSource[String] = {
        createMysqlSource(host, port, username, password, database, table)
    }
}
