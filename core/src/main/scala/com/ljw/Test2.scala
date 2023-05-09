package com.ljw

import com.ljw.utils.FlinkEnvUtil
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.createTypeInformation

import java.util

object Test2 {
  def main(args: Array[String]): Unit = {
    val env = FlinkEnvUtil.createLocalEnv()
    val input = env.fromElements("f1", "f2").setParallelism(1)
    val pattern = Pattern.begin[String]("start")
      .where(_.startsWith("f"))
    val patternStream = CEP.pattern(input, pattern)
    val out = patternStream.select(new PatternSelectFunction[String,String] {
      override def select(pattern: util.Map[String, util.List[String]]): String = {
        pattern.get("start").get(0)
      }
    })
    out.print()
    env.execute()
  }
}
