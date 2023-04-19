package com.ljw.app.dim

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.ljw.bean.TableProcessConfig
import com.ljw.common.PhoenixConfig
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.{Collector, StringUtils}
import org.apache.logging.log4j.{LogManager, Logger}

import java.sql.{Connection, DriverManager}
import scala.collection.immutable.HashSet
import scala.collection.JavaConversions._
class ConfigBroadProcFunc extends
  BroadcastProcessFunction[JSONObject, String, JSONObject] {
  private var connection: Connection = _
  private val LOG: Logger = LogManager.getLogger(classOf[ConfigBroadProcFunc])

  override def open(parameters: Configuration): Unit = {
    Class.forName(PhoenixConfig.PHOENIX_DRIVER)
    connection = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER)
    LOG info s"connection open!"
  }

  override def processElement(in1: JSONObject,
                              ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#ReadOnlyContext, collector: Collector[JSONObject]): Unit = {
    val state = ctx getBroadcastState ConfigBroadProcFunc.mapDesc
    val processConfig = state.get(in1.getString("table"))
    val set = processConfig.columns
    if (processConfig != null) {
      val nObject = in1.getJSONObject("data")
      nObject.keySet().removeIf(!set.contains(_))
      in1 put ("sinkTable" , processConfig.sinkTable)
      collector collect in1
    } else {
      LOG debug s"data filter , key is ${in1.getString("table")}"
    }
  }

  def checkTable(tableConfig: TableProcessConfig): Unit = {
    if (StringUtils.isNullOrWhitespaceOnly(tableConfig.sinkPk)) {
      tableConfig.sinkPk = "id"
    }
    if (tableConfig.sinkExtend == null) {
      tableConfig.sinkExtend = ""
    }
    val columns = tableConfig.sinkColumns.split(",", -1)
      .filter(_ != tableConfig.sinkPk)
      .map(_ + " varchar")
      .mkString(",")
    val sql = s"create table if not exists ${PhoenixConfig
      .HBASE_SCHEMA}.${tableConfig.sinkTable}" +
      s" (${tableConfig.sinkPk} varchar primary key , $columns) " +
      s"${tableConfig.sinkExtend} "
    LOG info s"receive table change, new sql is $sql"
    val statement = connection.prepareStatement(sql)
    statement.execute()
    statement.close()

  }

  /**
   *
   * cdc output example(delete)
   * {"before":{"source_table":"b","sink_table":"b","sink_columns":"b",
   * "sink_pk":"b","sink_extend":"b"},"after":null,
   * "source":{"version":"1.6.4.Final",
   * "connector":"mysql","name":"mysql_binlog_source",
   * "ts_ms":1681399423000,"snapshot":"false",
   * "db":"gmall_config","sequence":null,"table":"table_process",
   * "server_id":1,"gtid":null,"file":"mysql-bin.000008","pos":4952,
   * "row":0,"thread":null,"query":null},"op":"d","ts_ms":1681399423228,"transaction":null}
   */
  override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[JSONObject, String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {

    val nJson = JSON.parseObject(in2)
    val after = nJson.getString("after")
    val state = context getBroadcastState ConfigBroadProcFunc.mapDesc
    if (after == null)
      state remove JSON.parseObject(nJson.getString("before"))
        .getString("source_table")
    else {
      val tableConfig = new TableProcessConfig(after)
      checkTable(tableConfig)
      tableConfig.columns = new HashSet ++ tableConfig.sinkColumns.split(",").toSet
      state.put(tableConfig.sourceTable, tableConfig)
    }
  }

  override def close(): Unit = {
    connection.close()
  }
}

object ConfigBroadProcFunc {
  val mapDesc = new MapStateDescriptor[String,
    TableProcessConfig]("dim-config", classOf[String], classOf[TableProcessConfig])
}
