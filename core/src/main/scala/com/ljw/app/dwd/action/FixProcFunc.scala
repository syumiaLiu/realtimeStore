package com.ljw.app.dwd.action

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class FixProcFunc extends KeyedProcessFunction[String, JSONObject, JSONObject] {
  var firstViewDate: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    firstViewDate = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastLoginDt", classOf[Long]))
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val is_new = value.getJSONObject("common").getString("is_new")
    val fd = firstViewDate.value()
    val ts = value getLong "ts"
    if (is_new == "1") {
      if (fd == 0) firstViewDate update ts
      else {
        if (fd != ts) {
          value.getJSONObject("common").put("is_new", "0")
        }
      }
    }else{
      if(fd == 0 ) firstViewDate update ts-60*60*24*1000
    }

    out collect value
  }
}
