package com.ljw.app.dws

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.ljw.app.dwd.DWDConfig
import com.ljw.bean.TrafficHomeDetailPageViewBean
import com.ljw.utils.{DataFormatUtil, FlinkEnvUtil, KafkaUtils, MyClickchouseUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner
import org.apache.flink.util.Collector

import java.time.Duration

object DwsTrafficPageViewWindow {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    env.setParallelism(3)
    val source = env.fromSource(KafkaUtils.createDataStream(DWDConfig.page_topic, "DwsTrafficPageViewWindow"),
      WatermarkStrategy.noWatermarks(), "DwsTrafficPageViewWindow")
    val jsonStream: DataStream[JSONObject] = source.flatMap((in: String, out:Collector[JSONObject]) => {
      val nObj = JSON.parseObject(in)
      val pageId = nObj.getJSONObject("page").getString("page_id")
      if (pageId == "home" || pageId == "good_detail") out collect nObj
    })

    val jsonStreamWithTime: DataStream[JSONObject] = jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(element: JSONObject, recordTimestamp: Long): Long = element.getLong("ts")
      }))

    val outputStream = jsonStreamWithTime
      .keyBy(_.getJSONObject("common").getString("mid"))
      .flatMap(new DwsTrafficPageViewWindowFlatMap)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[TrafficHomeDetailPageViewBean] {
        override def reduce(value1: TrafficHomeDetailPageViewBean, value2: TrafficHomeDetailPageViewBean): TrafficHomeDetailPageViewBean = {
          value1.homeUvCt += value2.homeUvCt
          value1.goodDetailUvCt += value2.goodDetailUvCt
          value1
        }
      }, new AllWindowFunction[TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[TrafficHomeDetailPageViewBean], out: Collector[TrafficHomeDetailPageViewBean]): Unit = {
          val stt = DataFormatUtil.toYmdHms(window.getStart)
          val edt = DataFormatUtil.toYmdHms(window.getEnd)
          val ts = System.currentTimeMillis()
          for (x <- input) {
            x.stt = stt
            x.edt = edt
            x.ts = ts
            out collect x
          }
        }
      })

    outputStream.addSink(MyClickchouseUtil.getSinkFunction[TrafficHomeDetailPageViewBean]("insert into dws_traffic_page_view_window values (?,?,?,?,?)"))
    env.execute("DwsTrafficPageViewWindow")
  }

}
