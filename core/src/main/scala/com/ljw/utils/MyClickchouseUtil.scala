package com.ljw.utils

import com.ljw.app.dws.DWSConfig
import com.ljw.bean.TransientSink
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.sql.PreparedStatement

object MyClickchouseUtil {
  def getSinkFunction[T](sql: String): SinkFunction[T] = {
      JdbcSink.sink[T](sql ,
        new JdbcStatementBuilder[T]{
        override def accept(t: PreparedStatement, u: T): Unit = {
          val fields = u.getClass.getDeclaredFields
          var j = 0
          for (index <- 0 until fields.length) {
            val field = fields(index)
            field.setAccessible(true)
            val sink = field.getAnnotation(classOf[TransientSink])
            if(sink == null) {
              j += 1
              val value = field.get(u)
              t.setObject(j, value)
            }
          }
        }
      },
         new JdbcExecutionOptions.Builder().withBatchSize(5)
           .withBatchIntervalMs(1000L)
           .build()
        ,new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName(DWSConfig.CLICKHOUSE_DRIVER)
          .withUrl(DWSConfig.CLICKHOUSE_URL)
          .withUsername(DWSConfig.CLICKHOUSE_USER)
          .withPassword(DWSConfig.CLICKHOUSE_PASSWORD)
          .build()
      )
  }

}
