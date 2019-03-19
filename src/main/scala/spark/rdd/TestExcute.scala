package spark.rdd

import utils.LoggerLevels


/**
  * 这里是测试demo
  */
object TestExcute {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    for(i <- 1 to 9) {
      ExcuteAnalyze.test(i.toString)
    }
  }
}
