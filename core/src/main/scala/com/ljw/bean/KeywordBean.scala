package com.ljw.bean

import scala.beans.BeanProperty

class KeywordBean {
  @BeanProperty
  var stt: String = _
  @BeanProperty
  var edt: String = _
  @BeanProperty
  var source: String = _
  @BeanProperty
  var keyword: String = _
  @BeanProperty
  var keyword_count: Long = _
  @BeanProperty
  var ts: Long = _


  override def toString = s"KeywordBean(stt=$stt, edt=$edt, source=$source, keyword=$keyword, keyword_count=$keyword_count, ts=$ts)"
}
