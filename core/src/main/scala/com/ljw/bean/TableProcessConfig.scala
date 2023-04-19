package com.ljw.bean

import com.alibaba.fastjson2.JSON

import scala.beans.BeanProperty
import scala.collection.immutable


class TableProcessConfig extends Serializable {
  //来源表
  @BeanProperty
  var sourceTable: String = _
  //输出表
  @BeanProperty
  var sinkTable: String = _
  //输出字段
  @BeanProperty
  var sinkColumns: String = _
  //主键字段
  @BeanProperty
  var sinkPk: String = _
  //建表扩展
  @BeanProperty
  var sinkExtend: String = _

  var columns: immutable.HashSet[String] = _

  def this(str: String) {
    this()
    val nObject = JSON.parseObject(str)
    sourceTable = nObject.getString("source_table")
    sinkTable = nObject.getString("sink_table")
    sinkColumns = nObject.getString("sink_columns")
    sinkPk = nObject.getString("sink_pk")
    sinkExtend = nObject.getString("sink_extend")
  }
}
