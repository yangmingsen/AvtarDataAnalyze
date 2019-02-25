package spark.rdd

import utils.LoggerLevels

object TestExcute {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    ExcuteAnalyze.test()
  }
}
