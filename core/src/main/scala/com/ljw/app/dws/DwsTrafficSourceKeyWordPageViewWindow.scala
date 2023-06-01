package com.ljw.app.dws

import com.ljw.app.dwd.DWDConfig
import com.ljw.function.IKSplitFunction
import com.ljw.utils.{FlinkEnvUtil, KafkaUtils, MyClickchouseUtil}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import com.ljw.bean.KeywordBean
import org.apache.flink.api.scala._

object DwsTrafficSourceKeyWordPageViewWindow {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    tEnv.createTemporarySystemFunction("ik_analyze",classOf[IKSplitFunction])
    tEnv.executeSql("create table page_log(" +
      "`page` map<string,string>," +
      "`ts` bigint," +
//      "row_time as TO_TIMESTAMP_LTZ(ts,3)," +
      "row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
      "WATERMARK FOR row_time as row_time - INTERVAL '5' SECOND " +
      ") " + KafkaUtils.createDDLSource(DWDConfig.page_topic , "dwsPage"))


    val searchT = tEnv.sqlQuery("select " +
      "page['item'] full_word, " +
      "current_watermark(row_time), " +
      "row_time " +
      "from page_log " +
      "where page['item'] is not null " +
      "and page['last_page_id'] = 'search' " +
      "and " +
      "page['item_type'] = 'keyword'")
    tEnv.createTemporaryView("search_table",searchT)


    val splitTable = tEnv.sqlQuery("" +
      "select word ,row_time,current_watermark(row_time) from search_table,LATERAL TABLE (ik_analyze(full_word))")
    tEnv.createTemporaryView("split_table" , splitTable)


    val windowTable = tEnv sqlQuery (
      "select " +
        "DATE_FORMAT(TUMBLE_START(row_time,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
        "DATE_FORMAT(TUMBLE_END(row_time,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
        "'search' source," +
        "word keyword," +
        "count(*) keyword_count ," +
        "UNIX_TIMESTAMP()*1000 ts" +
        " from  " +
        "split_table " +
        "group by TUMBLE(row_time,INTERVAL '10' SECOND), word ;"
    )
    tEnv.createTemporaryView("windowTable" , windowTable)


    val countStream = tEnv.toAppendStream[KeywordBean](windowTable)

    countStream.addSink(MyClickchouseUtil.getSinkFunction[KeywordBean]("insert into dws_traffic_source_keyword_page_view_window values (?,?,?,?,?,?)"))



    env.execute("test")

  }

}
