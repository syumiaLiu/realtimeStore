package com.ljw.utils

import org.slf4j.LoggerFactory
import org.wltea.analyzer.core.IKSegmenter

import java.io.StringReader
import java.util

object KeyWordUtil {
  def analyze(text: String): java.util.List[String] = {
    val reader = new StringReader(text)
    val keyWordList = new util.ArrayList[String](8)
    val segmenter = new IKSegmenter(reader, true)
    try{
      var lexeme = segmenter.next()
      while(lexeme != null){
        keyWordList.add(lexeme.getLexemeText)
        lexeme = segmenter.next()
      }
    }catch {
      case e:Exception => println(s"text analyze errror! input text is $text")
    }
    keyWordList
  }

  def main(args: Array[String]): Unit = {
    val list = analyze("Apple iphone14 Pro Max) 256GB 深空灰色 移动联通电信5G手机 双卡双待")
    println(list)
  }

}
