import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment



object MysqlTest {
  def main(args: Array[String]): Unit = {
    val source: MySqlSource[String] = MySqlSource.builder[String]()
      .hostname("hadoop01")
      .port(3306)
      .username("maxwell")
      .password("maxwell")
      .databaseList("gmall_config")
      .tableList("gmall_config.table_process")
//      .startupOptions(StartupOptions.initial())
      .deserializer(new JsonDebeziumDeserializationSchema())
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(3000)
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySourceName")
      .print()
    env.execute()
  }
}
