package com.ljw.utils


import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

object DataFormatUtil {
  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   *
   * @param dateStr
   * @param isFull
   * @return timestamp +8
   */
  def toTimeStamp(dateStr: String ,isFull: Boolean):Long = {
    if(!isFull){
      LocalDateTime.parse(s"$dateStr 00:00:00" ,dtfFull ).toInstant(ZoneOffset.of("+8")).toEpochMilli
    }else
      LocalDateTime.parse(dateStr ,dtfFull ).toInstant(ZoneOffset.of("+8")).toEpochMilli
  }

  def toTimeStamp(dataStr: String) : Long = {
    toTimeStamp(dataStr,false)
  }

  def toYmdHms(timeStamp: Long) : String = {
      LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp),ZoneOffset.of("+8")).format(dtfFull)

  }

  def toYmd(timeStamp: Long): String = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneOffset.of("+8")).format(dtf)

  }
  
}
