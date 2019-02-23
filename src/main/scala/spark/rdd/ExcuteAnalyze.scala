package spark.rdd

import top.ccw.avtar.redis.RedisClient

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
    *启动分析方法，无需方向参数。
    * 方向从Redis提取
    */
  def startAnalyze(): Unit = {

    //获取存储在Redis的方向命令（这个方向命令是springboot后台存放的）
    val direction = RedisClient.getValue("msgCmd","direction")


  }

  /***
    * 启动分析方法，需要传入 方向指令 cmd
    * @param direcion 方向指令:表示当前需要分析程序分析什么方向的数据
    */
  def startAnalyze(direcion: String): Unit = {

  }


  /***
    * 读取数据
    */
  def dataIn(): Unit = {

  }

  /***
    * 实时转态分析
    * @return
    */
  def currentStatus(): Boolean = {
    true
  }


  /***
    * 统计图表
    * @return
    */
  def statisticalGraph(): Boolean = {
    true
  }

}
