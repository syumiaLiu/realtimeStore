package com.ljw.app.dwd.action

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

class SeparatedProc extends ProcessFunction[JSONObject, String] {
  val startTag = new OutputTag[String]("startTag")
  val displayTag = new OutputTag[String]("displayTag")
  val actionTag = new OutputTag[String]("actionTag")
  val errorTag = new OutputTag[String]("errorTag")

  override def processElement(value: JSONObject, ctx: ProcessFunction[JSONObject, String]#Context, out: Collector[String]): Unit = {
    val error = value getJSONObject "error"
    if (error != null) ctx output(errorTag, error.toJSONString())
    value remove "error"

    val start = value getJSONObject "start"
    if(start != null) ctx output (startTag,start.toJSONString())
    else{
      //display data
      val page = value getJSONObject "page"
      val common = value getJSONObject "common"
      val ts = value getLong "ts"
      val displays = value getJSONArray "displays"
      if(displays != null){
        for(i <- 0 until   displays.size()){
          val display = displays.getJSONObject(i)
          val nObject = new JSONObject()
          nObject put ("display" , display)
          nObject put ("common" , common)
          nObject put ("ts" , ts)
          nObject put ("page" , page)
          ctx output (displayTag , nObject.toJSONString())
        }
      }

      //action data
      val actions = value getJSONArray "actions"
      if(actions != null){
        for(i <- 0 until actions.length){
          val action = actions.getJSONObject(i)
          val nObject = new JSONObject()
          nObject put("action", action)
          nObject put("common", common)
          nObject put("ts", ts)
          nObject put("page", page)
          ctx output (actionTag , nObject.toJSONString())
        }
      }
    }

    value remove "actions"
    value remove "displays"
    out collect value.toJSONString()

  }
}
