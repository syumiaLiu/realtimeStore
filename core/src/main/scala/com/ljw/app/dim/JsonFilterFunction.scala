package com.ljw.app.dim

import com.alibaba.fastjson2.{JSON, JSONObject}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class JsonFilterFunction extends ProcessFunction[String, JSONObject] {
  val inValidOutput = new OutputTag[String]("invalidData")

  val list = List("insert","update","bootstrap-insert")
  override def processElement(i: String, context: ProcessFunction[String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
      if(JSON.isValid(i)){
        val jObject = JSON.parseObject(i)
        val typeName = jObject.getString("type")
        if(list.contains(typeName)){
          collector collect  jObject
        }
      }else{
        context output (inValidOutput,i)
      }
  }
}
