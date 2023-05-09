package com.ljw.app.dwd.visit

import com.alibaba.fastjson2.JSON
import com.ljw.utils.FlinkEnvUtil._
import com.ljw.utils.KafkaUtils
import org.apache.flink.api.scala._
import com.ljw.app.dwd.DWDConfig._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.DataStream
/**
 * this program data depends on {@link com.ljw.app.dwd.action.BaseLogApp}
 */
object UniqueVisitDetail {
  def main(args: Array[String]): Unit = {
    val env = createLocalEnv()
    checkpointConfigInti(env)
    useHDFSBackend(env)
    val kafkaUtils = new KafkaUtils
    val source: DataStream[String] = env fromSource(kafkaUtils.createDataStream(page_topic), WatermarkStrategy.noWatermarks(), "page source")
    val out = source.map(JSON.parseObject(_))
      .filter(_.getJSONObject("page").getString("last_page_id") == null)
      .keyBy(_.getJSONObject("common").getString("mid"))
      .filter(new UniqueVisitProc)
      .map(_.toJSONString())

    val sink = kafkaUtils.createSink(uv_topic)
    out.sinkTo(sink)
    env.execute("uv filter")
  }

}
