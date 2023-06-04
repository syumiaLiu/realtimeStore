package com.ljw.app.dws

import com.alibaba.fastjson2.JSON
import com.ljw.app.dwd.DWDConfig
import com.ljw.bean.TrafficPageViewBean
import com.ljw.utils.{DataFormatUtil, FlinkEnvUtil, KafkaUtils, MyClickchouseUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
object DwsTrafficVcChArIsNewPageViewWindow {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    //only for test. In accordance with practice，it will be set to  number of partitions.
    env.setParallelism(1)
    val uvSource = env.fromSource(KafkaUtils.createDataStream(DWDConfig.uv_topic, "dws_traffic_window"), WatermarkStrategy.noWatermarks(), "uv source")
    val ujdSource = env.fromSource(KafkaUtils.createDataStream(DWDConfig.bounceRateTopic, "dws_traffic_window"), WatermarkStrategy.noWatermarks(), "ujd Source")
    val pageSource = env.fromSource(KafkaUtils.createDataStream(DWDConfig.page_topic, "dws_traffic_window"), WatermarkStrategy.noWatermarks(), "page source")
    val beanFunc = (x:String) =>{
      val nObject = JSON.parseObject(x)
      val common = nObject.getJSONObject("common")
      val bean = new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, 0L,
        0L, 0L, 0L, nObject.getLong("ts"))
      bean
    }
    val trafficPageViewWithUV = uvSource.map(x => {
      val bean = beanFunc(x)
      bean.uvCt = 1L
      bean
    })

    val trafficPageViewWithUjd = ujdSource.map(x => {
      val bean = beanFunc(x)
      bean.ujCt = 1L
      bean
    })

    val trafficPageViewWithPage = pageSource.map(x => {
      val nObject = JSON.parseObject(x)
      val common = nObject.getJSONObject("common")
      val page = nObject.getJSONObject("page")
      var sv = 0L
      try {
        val lastPageId = page.getString("last_page_id")
        if (lastPageId == null) {
          sv = 1L
        }
      }catch {
        case e: NullPointerException => println(s"error json: $x")
      }

      val bean = new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, sv,
        1L, page.getLong("during_time"), 0L, nObject.getLong("ts"))
      bean
    })

//      no ujd data
    val allStream = trafficPageViewWithPage.union( trafficPageViewWithUV,trafficPageViewWithUjd)
//        val allStream = trafficPageViewWithPage.union( trafficPageViewWithUV)

    val streamWithWatermark: DataStream[TrafficPageViewBean] = allStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[TrafficPageViewBean](Duration
      .ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[TrafficPageViewBean] {
        override def extractTimestamp(element: TrafficPageViewBean, recordTimestamp: Long): Long = element.ts
      }))

    val resStream = streamWithWatermark.keyBy(bean => (bean.ar, bean.ch, bean.vc, bean.isNew))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[TrafficPageViewBean] {
        override def reduce(value1: TrafficPageViewBean, value2: TrafficPageViewBean): TrafficPageViewBean = {
          value1 += value2
        }
      }, new WindowFunction[TrafficPageViewBean, TrafficPageViewBean, (String, String, String, String), TimeWindow] {
        override def apply(key: (String, String, String, String), window: TimeWindow, input: Iterable[TrafficPageViewBean], out: Collector[TrafficPageViewBean]): Unit = {
          val start = DataFormatUtil.toYmdHms(window.getStart)
          val end = DataFormatUtil.toYmdHms(window.getEnd)
          input.foreach(element => {
            element.stt = start
            element.edt = end
            out collect element
          })
        }
      })

    resStream.print()

//    resStream.addSink(MyClickchouseUtil.getSinkFunction[TrafficPageViewBean]("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values (?,?,?,?,?,?,?,?,?,?,?,?)"))
    env.execute()
  }
}
