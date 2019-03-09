package spark.rdd

import entity.JobDataEntity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.rdd.current._
import spark.rdd.statistical.EducationCompanyTypeJobNumAnalyze
import top.ccw.avtar.redis.RedisClient

/** *
  * <p>共分析2个主题：实时状态、统计图表</p>
  * 流程：
  * 1、从HBase或者MySQL读取数据
  * 2、读入数据后首先分析实时状态
  * 3、实时状态分析完毕后、计算统计图表
  *
  * @author yangmingsen
  */
object ExcuteAnalyze {

  //初始化环境
  val conf = new SparkConf().setAppName("ExcuteAnalyze").set("spark.driver.host", "localhost").setMaster("local[4]")
  val sc = new SparkContext(conf)


  def test(): Unit = {

    startAnalyze("10") //test 目前方向是 软件工程

  }

  /** *
    * 启动分析方法，无需方向参数。
    * 方向从Redis提取
    */
  def startAnalyze(): Unit = {

    //获取存储在Redis的方向命令（这个方向命令是springboot后台存放的）
    val direction = RedisClient.getValue("msgCmd", "direction")

    //执行分析程序
    excuteAnalyze(direction)

  }

  /** *
    * 启动分析方法，需要传入 方向指令 cmd
    *
    * @param direcion 方向指令:表示当前需要分析程序分析什么方向的数据
    */
  def startAnalyze(direcion: String): Unit = {
    excuteAnalyze(direcion)
    sc.stop()
  }


  private def excuteAnalyze(direcion: String): Unit = {

    //读取数据
    val jobsData = dataIn()

    //进入时状态分析
    //currentStatus(jobsData,direcion)

    //进入统计图表分析
    statisticalGraph(jobsData, direcion)

  }


  /** *
    * 读取数据
    */
  private def dataIn(): RDD[JobDataEntity] = {

    //测试读取MySQL数据
    dataInFromMySQL()

    //读取HBase数据

  }

  private def dataInFromMySQL(): RDD[JobDataEntity] = {

    val sqlContext = new SQLContext(sc)

    val jdbcDF = sqlContext.read.format("jdbc").
      options(Map("url" -> "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com/job_data?characterEncoding=utf8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_job_info_new", "user" -> "user", "password" -> "Group1234")).load()
    jdbcDF.registerTempTable("tb_job_info_new")

    /*val jdbcDF = sqlContext.read.format("jdbc").
      options(Map("url" -> "jdbc:mysql://127.0.0.1:3306/job_data?characterEncoding=utf8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_job_info_new", "user" -> "root", "password" -> "ymsyms")).load()
    jdbcDF.registerTempTable("tb_job_info_new")*/

    val jobDF = sqlContext.sql("SELECT * FROM `tb_job_info_new` WHERE id BETWEEN 1 AND 100")

    val rdd1 = jobDF.map(x => {
      val direction = x.getInt(1).toString
      val jobName = x.getString(2)
      val companyName = x.getString(3)
      val jobSiteProvinces = x.getString(4)
      val jobSite = x.getString(5)
      val jobSalaryMin = x.getString(6)
      val jobSalaryMax = x.getString(7)
      val relaseDate = x.getString(8)
      val educationLevel = x.getString(9)
      val workExper = x.getString(10)
      val companyWelfare = x.getString(11)
      val jobResp = x.getString(12)
      val jobRequire = x.getString(13)
      val companyType = x.getString(14)
      val companyPeopleNum = x.getString(15)
      val companyBusiness = x.getString(16)

      JobDataEntity(direction, jobName, companyName, jobSiteProvinces, jobSite, jobSalaryMin, jobSalaryMax, relaseDate, educationLevel,
        workExper, companyWelfare, jobResp, jobRequire, companyType, companyPeopleNum, companyBusiness)
    })

    println("get Data from MySQL" + "AND data size = " + rdd1.collect().toList.size)

    rdd1
  }

  /** *
    * 实时状态分析
    *
    * @return
    */
  private def currentStatus(jobsData: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    //分析TimeSalary
    TimeSalaryAnalyze.start(jobsData, jobtypeTwoId)

    //分析SalarySite
    SalarySiteAnalyze.start(jobsData, jobtypeTwoId)

    //分析ProvinceJobNum
    ProvinceJobNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 JobNameRank
    JobNameRankAnalyze.start(jobsData, jobtypeTwoId)

    //分析 EducationJobNum
    EducationJobNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 CompanyTypeJobNumSalaryAve
    CompanyTypeJobNumSalaryAveAnalyze.start(jobsData, jobtypeTwoId)


  }


  /** *
    * 统计图表
    *
    * @return
    */
  private def statisticalGraph(jobsData: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    //分析 Company_businessNum
    //Company_businessNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 SalaryWorkExperJobNumAve
    //SalaryWorkExperJobNumAveEntityAnalyze.start(jobsData, jobtypeTwoId)

    //分析 EducationCompanyTypeJobNum
    EducationCompanyTypeJobNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 EducationJobNumSalaryAve
    //EducationJobNumSalaryAveAnalyze.start(jobsData, jobtypeTwoId)

    //分析 CompanyTypeNumAve
    //CompanyTypeNumAveAnalyze.start(jobsData, jobtypeTwoId)

    //分析 JobNameNum
    //JobNameNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 EducationSalaryAve
    //EducationSalaryAveAnalyze.start(jobsData, jobtypeTwoId)

    //分析 CompanyTypeSalaryAve
    //CompanyTypeSalaryAveAnalyze.start(jobsData, jobtypeTwoId)
  }

}
