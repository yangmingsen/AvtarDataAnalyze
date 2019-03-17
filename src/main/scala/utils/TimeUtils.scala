package utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * program: AvtarDataAnalyze
  * author: ljq
  * create: 2019-03-17 19:54
  **/
object TimeUtils {
  def getNowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val nowdate = dateFormat.format(now)
    nowdate
  }
  def isWeekRange(date: String): Int = {
    val day = date.substring(5, date.length)
    if (day <= "02-18")
      0
    else if (day > "02-18" & day <= "02-25")
      1
    else if (day > "02-25" && day <= "03-02")
      2
    else if (day > "03-02" && day <= "03-09")
      3
    else
      4
  }
}
