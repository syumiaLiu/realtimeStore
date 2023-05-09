package com.ljw.app.dwd.action

import com.alibaba.fastjson2.JSON
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

class JsonFilterProc extends ProcessFunction[String,String] {
  val dirtyOut = new OutputTag[String]("dirty stream")

  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
    if (JSON.isValid(value))
      out collect value
    else
      ctx output(dirtyOut, value)
  }
}
