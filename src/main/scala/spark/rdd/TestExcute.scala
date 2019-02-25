package spark.rdd

import utils.LoggerLevels


/**
  * 这里是测试demo
  */
object TestExcute {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    ExcuteAnalyze.test()
  }
}
