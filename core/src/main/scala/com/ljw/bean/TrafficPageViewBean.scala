package com.ljw.bean

import scala.beans.BeanProperty

class TrafficPageViewBean {
  // 窗口起始时间
  @BeanProperty
  var stt: String = _
  // 窗口结束时间
  @BeanProperty
  var edt: String = _
  // app 版本号
  @BeanProperty
  var vc: String = _
  // 渠道
  @BeanProperty
  var ch: String = _
  // 地区
  @BeanProperty
  var ar: String = _
  // 新老访客状态标记
  @BeanProperty
  var isNew: String = _
  // 独立访客数
  @BeanProperty
  var uvCt = 0L
  // 会话数
  @BeanProperty
  var svCt = 0L
  // 页面浏览数
  @BeanProperty
  var pvCt = 0L
  // 累计访问时长
  @BeanProperty
  var durSum = 0L
  // 跳出会话数
  @BeanProperty
  var ujCt = 0L
  // 时间戳
  @BeanProperty
  var ts = 0L

  def this(stt: String, edt: String, vc: String, ch: String, ar: String, isNew: String, uvCt: Long, svCt: Long, pvCt: Long, durSum: Long, ujCt: Long, ts: Long) {
    this()
    this.stt = stt
    this.edt = edt
    this.vc = vc
    this.ch = ch
    this.ar = ar
    this.isNew = isNew
    this.uvCt = uvCt
    this.svCt = svCt
    this.pvCt = pvCt
    this.durSum = durSum
    this.ujCt = ujCt
    this.ts = ts
  }

  def += (value2: TrafficPageViewBean): TrafficPageViewBean ={
    this.uvCt += value2.uvCt
    this.svCt += value2.svCt
    this.pvCt += value2.pvCt
    this.durSum += value2.durSum
    this.ujCt += value2.ujCt
    this
  }

  override def toString = s"TrafficPageViewBean(stt=$stt, edt=$edt, vc=$vc, ch=$ch, ar=$ar, isNew=$isNew, uvCt=$uvCt, svCt=$svCt, pvCt=$pvCt, durSum=$durSum, ujCt=$ujCt, ts=$ts)"
}
