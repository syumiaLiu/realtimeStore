package com.ljw.bean

import scala.beans.BeanProperty

class TrafficHomeDetailPageViewBean {
  // 窗口起始时间
  @BeanProperty
  var stt: String = null

  // 窗口结束时间
  @BeanProperty
  var edt: String = null

  // 首页独立访客数
  @BeanProperty
  var homeUvCt = 0L

  // 商品详情页独立访客数
  @BeanProperty
  var goodDetailUvCt = 0L

  // 时间戳
  @BeanProperty
  var ts = 0L


  def this(stt: String ,edt: String,homeUvCt: Long ,goodDetailUvCt: Long , ts: Long){
    this()
    this.stt = stt
    this.edt = edt
    this.homeUvCt = homeUvCt
    this.goodDetailUvCt = goodDetailUvCt
    this.ts = ts
  }
}
