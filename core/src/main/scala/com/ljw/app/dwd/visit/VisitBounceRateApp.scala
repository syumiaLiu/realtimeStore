package com.ljw.app.dwd.visit

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.ljw.app.dwd.DWDConfig._
import com.ljw.utils.FlinkEnvUtil._
import com.ljw.utils.KafkaUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util

object VisitBounceRateApp {
  def main(args: Array[String]): Unit = {
    val env = createLocalEnv()
    checkpointConfigInti(env)
    useHDFSBackend(env)
    val source: DataStream[String] = env fromSource(KafkaUtils.createDataStream(page_topic), WatermarkStrategy.noWatermarks(), "page source")

    val mappedStream: DataStream[JSONObject] = source.map(JSON.parseObject(_))
    val withwaterMarkStream: DataStream[JSONObject] = mappedStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
          override def extractTimestamp(element: JSONObject, recordTimestamp: Long): Long = element.getLong("ts")
        }
        ))

    val keyedStream = withwaterMarkStream.keyBy(_.getJSONObject("common").getString("mid"))

    val pattern = Pattern.begin[JSONObject]("first")
      .where(_.getJSONObject("page").getString("last_page_id") == null)
      .next("second")
      .where(_.getJSONObject("page").getString("last_page_id") == null)
      .within(Time.seconds(10L))

    val patternStream = CEP.pattern(keyedStream, pattern)
    val tag = new OutputTag[JSONObject]("timeoutTag")

    val out = patternStream.flatSelect(tag, new PatternFlatTimeoutFunction[JSONObject, JSONObject] {
      override def timeout(pattern: util.Map[String, util.List[JSONObject]], timeoutTimestamp: Long, out: Collector[JSONObject]): Unit = {
        val element = pattern.get("first").get(0)
        out collect element
      }
    },new PatternFlatSelectFunction[JSONObject,JSONObject] {
      override def flatSelect(pattern: util.Map[String, util.List[JSONObject]], out: Collector[JSONObject]): Unit = {
        val element = pattern.get("first").get(0)
        out collect element
      }
    })

    val timeout = out.getSideOutput(tag)
//    out.print()
    val sink = KafkaUtils.createSink(bounceRateTopic)
    val output = timeout.union(out).map(_.toJSONString())
    output.sinkTo(sink)
    env.execute()
  }
}
