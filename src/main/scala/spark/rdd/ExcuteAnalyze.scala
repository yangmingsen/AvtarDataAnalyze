package spark.rdd

/***
  * <p>共分析2个主题：实时状态、统计图表</p>
  * 流程：
  *   1、从HBase或者MySQL读取数据
  *   2、读入数据后首先分析实时状态
  *   3、实时状态分析完毕后、计算统计图表
  *
  */
object ExcuteAnalyze {

  /***
    * 启动分析方法，需要传入 方向指令 cmd
    * @param cmd 方向指令:表示当前需要分析程序分析什么方向的数据
    */
  def startAnalyze(cmd: String): Unit = {

  }



}
