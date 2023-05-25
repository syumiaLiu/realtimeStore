package com.ljw.app.dwd.action

import com.alibaba.fastjson2.JSON
import com.ljw.app.dwd.DWDConfig._
import com.ljw.utils.FlinkEnvUtil._
import com.ljw.utils.KafkaUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag

object BaseLogApp {
  def main(args: Array[String]): Unit = {
    val env = createLocalEnv()
    checkpointConfigInti(env)
    useHDFSBackend(env)
    val source = env.fromSource(KafkaUtils.createDataStream(source_topic, groupId), WatermarkStrategy.noWatermarks(), "app log source")

    val dirtyOut = new OutputTag[String]("dirty stream")
    val cleanStream = source process new JsonFilterProc
    val dirtySink = KafkaUtils createSink dirty_topic
    cleanStream.getSideOutput(dirtyOut).sinkTo(dirtySink)

    val out = cleanStream.map(JSON.parseObject(_))
      .keyBy(_.getJSONObject("common").getString("mid"))
      .process(new FixProcFunc)
      .process(new SeparatedProc)


    val startTag = new OutputTag[String]("startTag")
    val displayTag = new OutputTag[String]("displayTag")
    val actionTag = new OutputTag[String]("actionTag")
    val errorTag = new OutputTag[String]("errorTag")

    val action_sink = KafkaUtils createSink action_topic
    val display_sink = KafkaUtils createSink display_topic
    val error_sink = KafkaUtils createSink error_topic
    val start_sink = KafkaUtils createSink start_topic
    val page_sink = KafkaUtils createSink page_topic

    out sinkTo page_sink
    out.getSideOutput(startTag).sinkTo(start_sink)
    out.getSideOutput(displayTag).sinkTo(display_sink)
    out.getSideOutput(actionTag).sinkTo(action_sink)
    out.getSideOutput(errorTag).sinkTo(error_sink)


    env.execute("app log")
  }
}
