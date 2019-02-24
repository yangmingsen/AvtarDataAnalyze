package spark.rdd

import entity.JobDataEntity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import top.ccw.avtar.redis.RedisClient

/***
  * <p>共分析2个主题：实时状态、统计图表</p>
  * 流程：
  *   1、从HBase或者MySQL读取数据
  *   2、读入数据后首先分析实时状态
  *   3、实时状态分析完毕后、计算统计图表
  *
  * @author yangmingsen
  */
object ExcuteAnalyze {

  //初始化环境
  val conf = new SparkConf().setAppName("ProvinceJobNum").setMaster("local[5]")
  val sc = new SparkContext(conf)



  /***
    *启动分析方法，无需方向参数。
    * 方向从Redis提取
    */
  def startAnalyze(): Unit = {


    //获取存储在Redis的方向命令（这个方向命令是springboot后台存放的）
    val direction = RedisClient.getValue("msgCmd","direction")

    //读取数据
    val jobData = dataIn()

    //进入时状态分析


    //进入统计图表分析

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
  def dataIn(): RDD[JobDataEntity] = {

    //测试读取MySQL数据
    dataInFromMySQL()

    //读取HBase数据

  }

  def dataInFromMySQL(): RDD[JobDataEntity] = {
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").
      options(Map("url" -> "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com/job_data?characterEncoding=utf8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_job_info_new", "user" -> "user", "password" -> "Group1234")).load()
    jdbcDF.registerTempTable("tb_job_info_new")

    val jobDF = sqlContext.sql("SELECT * FROM `tb_job_info_new`")

    val rdd1 = jdbcDF.map(x => {
      val direction = x.getString(1)
      val jobName = x.getString(2)
      val companyName = x.getString(3)
      val jobSite = x.getString(4)
      val jobSalaryMin = x.getString(5)
      val jobSalaryMax = x.getString(6)
      val relaseDate = x.getString(7)
      val educationLevel = x.getString(8)
      val workExper = x.getString(9)
      val companyWelfare = x.getString(10)
      val jobResp = x.getString(11)
      val jobRequire = x.getString(12)
      val companyType = x.getString(13)
      val companyPeopleNum = x.getString(14)
      val companyBusiness = x.getString(15)

      JobDataEntity(direction,jobName,companyName,jobSite,jobSalaryMin,jobSalaryMax,relaseDate,educationLevel,
        workExper,companyWelfare,jobResp,jobRequire,companyType,companyPeopleNum,companyBusiness)
    })

    rdd1
  }

  /***
    * 实时状态分析
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
