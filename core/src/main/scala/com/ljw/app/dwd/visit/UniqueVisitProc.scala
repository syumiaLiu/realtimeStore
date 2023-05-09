package com.ljw.app.dwd.visit

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction

import scala.runtime.RichFloat

class UniqueVisitProc extends RichFilterFunction[JSONObject]{
  var lastVisitTime: ValueState[Long] = _
  override def open(parameters: Configuration): Unit = {
    val stateDesc = new ValueStateDescriptor[Long]("last view date", classOf[Long])
    val ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .cleanupFullSnapshot()
      .build()
    stateDesc.enableTimeToLive(ttlConfig)
    lastVisitTime = getRuntimeContext.getState(stateDesc)

  }
  override def filter(value: JSONObject): Boolean = {
    val ts = value.getLong("ts")
     if(lastVisitTime == 0 || ts != lastVisitTime.value()){
       lastVisitTime update ts
       true
     }else
       false
  }
}
