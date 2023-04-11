import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.debezium.{JsonDebeziumDeserializationSchema, StringDebeziumDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment



object MysqlTest {
  def main(args: Array[String]): Unit = {
    val source: MySqlSource[String] = MySqlSource.builder[String]()
      .hostname("112.124.36.125")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("gmall")
      .tableList("gmall.*")
      .deserializer(new JsonDebeziumDeserializationSchema())
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(3000)
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySourceName")
      .print()
    env.execute()
  }
}
