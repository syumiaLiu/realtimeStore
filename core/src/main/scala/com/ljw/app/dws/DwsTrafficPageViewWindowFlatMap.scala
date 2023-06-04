package com.ljw.app.dws

import com.alibaba.fastjson2.JSONObject
import com.ljw.bean.TrafficHomeDetailPageViewBean
import com.ljw.utils.DataFormatUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class DwsTrafficPageViewWindowFlatMap extends RichFlatMapFunction[JSONObject, TrafficHomeDetailPageViewBean] {
  var homeLastVisitDt: ValueState[String] = _
  var detailLastVisitDt: ValueState[String] = _

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
      .setUpdateType(UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateVisibility.NeverReturnExpired)
      .build()
    val homeLastVisitDtDesc = new ValueStateDescriptor[String]("homeLastVisitDtDesc", classOf[String])
    val detailLastVisitDtDesc = new ValueStateDescriptor[String]("detailLastVisitDtDesc", classOf[String])
    homeLastVisitDtDesc enableTimeToLive ttlConfig
    detailLastVisitDtDesc enableTimeToLive ttlConfig

    homeLastVisitDt = getRuntimeContext getState homeLastVisitDtDesc
    detailLastVisitDt = getRuntimeContext getState detailLastVisitDtDesc

  }

  override def flatMap(value: JSONObject, out: Collector[TrafficHomeDetailPageViewBean]): Unit = {
    val pageId = value.getJSONObject("page").getString("page_id")
    var homeUvCnt = 0L
    var detailUvCnt = 0L
    val date = DataFormatUtil.toYmd(value.getLong("ts"))

    pageId match{
      case "home" =>
        val dt = homeLastVisitDt.value()
        if(dt == null || dt != date){
          homeUvCnt = 1L
          homeLastVisitDt update date
        }
      case "good_detail" =>
        val dt = detailLastVisitDt.value()
        if (dt == null || dt != date) {
          detailUvCnt = 1L
          detailLastVisitDt update date
        }
      case _ =>
    }
    if((detailUvCnt ^ homeUvCnt) != 0L){
      out collect new TrafficHomeDetailPageViewBean("","",homeUvCnt,detailUvCnt , 0L)
    }


  }
}
