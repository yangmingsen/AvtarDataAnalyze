package spark.rdd

import java.util.concurrent.{ExecutorService, Executors}

import com.alibaba.fastjson.JSON
import entity.JobDataEntity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import spark.rdd.current._
import spark.rdd.statistical._
import top.ccw.avtar.db.Update
import top.ccw.avtar.redis.RedisClient
import top.ccw.avtar.websocket.WebSocketClient

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
  val conf = new SparkConf().setAppName("ExcuteAnalyze").set("spark.driver.host", "localhost").setMaster("local[*]")
  //在spark中自动创建es中的索引
  conf.set("es.index.auto.create", "true")
  //设置在Spark 中连接 es的url和端口
  conf.set("es.nodes", "10.0.0.28")
  conf.set("es.port", "9200")

  val sc = new SparkContext(conf)

  def test(direction: String): Unit = {

    startAnalyze(direction) //test
    //stopAnalyze()
  }

  /** *
    * 启动分析方法，无需方向参数。
    * 方向从Redis提取
    */
  def startAnalyze(): Unit = {

    //获取存储在Redis的方向命令（这个方向命令是springboot后台存放的）
    val direction = RedisClient.getNowAnalyzeValue

    println("direction in Redis : " + direction)

    if (direction != null && direction != "") {
      //执行分析程序
      excuteAnalyze(direction)
    }

  }

  /** *
    * 启动分析方法，需要传入 方向指令 cmd
    *
    * @param direcion 方向指令:表示当前需要分析程序分析什么方向的数据
    */
  def startAnalyze(direcion: String): Unit = {
    excuteAnalyze(direcion)

  }

  def stopAnalyze(): Unit = {
    sc.stop()
  }


  private def excuteAnalyze(direcion: String): Unit = {

    //更新当前数据插入 方向
    Update.setUpdateInfo(Integer.parseInt(direcion))

    //读取数据
    val jobsData = dataIn(direcion)

    //进入时状态分析
    currentStatus(jobsData, direcion)

    //进入统计图表分析
    //statisticalGraph(jobsData, direcion)

    println("分析完毕...")

  }


  /** *
    * 描述：分析读取数据
    *
    * 1、从mysql
    * 2、从hbase
    * 3、从elasticsearch
    *
    */
  private def dataIn(jobtypeTwoId: String): RDD[JobDataEntity] = {


    //从MySQL读取数据
    //dataInFromMySQL(jobtypeTwoId)

    //读取HBase数据

    //从ES中读取数据
    dataInFromElasticsearch(jobtypeTwoId)

    //从ES中读取数据
    //dataInFromElasticsearchToday(jobtypeTwoId)

  }


  /** *
    * 实时状态分析
    *
    * @return
    */
  private def currentStatus(jobsData: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    //分析TimeSalary
    //TimeSalaryAnalyze.start(jobsData, jobtypeTwoId)

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

    WordCloudAnalyze.start(jobsData,jobtypeTwoId)

  }


  /** *
    * 统计图表
    *
    * @return
    */
  private def statisticalGraph(jobsData: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    //分析 Company_businessNum
    //CompanyBusinessNumAnalyze.start(jobsData, jobtypeTwoId)

    //分析 SalaryWorkExperJobNumAve
    //SalaryWorkExperJobNumAveEntityAnalyze.start(jobsData, jobtypeTwoId)

    //分析 EducationCompanyTypeJobNum
    //EducationCompanyTypeJobNumAnalyze.start(jobsData, jobtypeTwoId)

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

    //中间数据层 IntermediateDataLayer
    //IntermediateDataLayerAnalyze.start(jobsData, jobtypeTwoId)

    //分析词云
    //WordCloudAnalyze.start(jobsData, jobtypeTwoId)
    val threadPool: ExecutorService = Executors.newFixedThreadPool(4)
    try {
      for (i <- 1 to 10) {
        //threadPool.submit(new ThreadDemo("thread"+i))
        threadPool.execute(new ThreadDemo(i, jobsData, jobtypeTwoId))
      }
    } finally {
      threadPool.shutdown()
    }
  }

  private def Scheduling(i: Int, jobsData: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    if (i == 1) {
      CompanyBusinessNumAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 2) {
      SalaryWorkExperJobNumAveEntityAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 3) {
      EducationCompanyTypeJobNumAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 4) {
      EducationJobNumSalaryAveAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 5) {
      CompanyTypeNumAveAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 6) {
      JobNameNumAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 7) {
      EducationSalaryAveAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 8) {
      CompanyTypeSalaryAveAnalyze.start(jobsData, jobtypeTwoId)
    }
    else if (i == 9) {
      IntermediateDataLayerAnalyze.start(jobsData, jobtypeTwoId)
    }
    else {
      WordCloudAnalyze.start(jobsData, jobtypeTwoId)
    }
  }

  class ThreadDemo(i: Int, jobsData: RDD[JobDataEntity], jobtypeTwoId: String) extends Runnable {
    override def run() {
      Scheduling(i, jobsData, jobtypeTwoId)
    }
  }

  private def dataInFromMySQL(jobtypeTwoId: String): RDD[JobDataEntity] = {

    val sql = "SELECT * FROM `tb_jobinfo_data` WHERE AND relase_date='2019-03-23' AND  direction=" + jobtypeTwoId
    val sqlContext = new SQLContext(sc)

    //    val jdbcDF = sqlContext.read.format("jdbc").
    //      options(Map("url" -> "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com/job_data?characterEncoding=utf8&useSSL=false",
    //        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_job_info_new", "user" -> "user", "password" -> "AvtarGroup1234")).load()
    //    jdbcDF.registerTempTable("tb_job_info_new")

    //    val jdbcDF = sqlContext.read.format("jdbc").
    //      options(Map("url" -> "jdbc:mysql://10.0.0.28:3306/job_data?characterEncoding=utf8&useSSL=false",
    //        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_jobinfo_data", "user" -> "yms", "password" -> "yms")).load()
    //    jdbcDF.registerTempTable("tb_jobinfo_data")

    val jdbcDF = sqlContext.read.format("jdbc").
      options(Map("url" -> "jdbc:mysql://localhost:3306/job_data?characterEncoding=utf8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "tb_jobinfo_data", "user" -> "yms", "password" -> "yms")).load()
    jdbcDF.registerTempTable("tb_jobinfo_data")

    val jobDF = sqlContext.sql(sql)

    val rdd1 = jobDF.map(x => {

      val id = x.getInt(0).toString
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
      val jobRequire = x.getString(12)
      val companyType = x.getString(13)
      val companyPeopleNum = x.getString(14)
      val companyBusiness = x.getString(15)

      JobDataEntity(id, direction, jobName, companyName, jobSiteProvinces, jobSite, jobSalaryMin, jobSalaryMax, relaseDate, educationLevel,
        workExper, companyWelfare, jobRequire, companyType, companyPeopleNum, companyBusiness)
    })



    //println("get Data from MySQL" + "AND data size = " + rdd1.collect().toList.size)

    rdd1
  }

  private def dataInFromElasticsearch(jobtypeTwoId: String): RDD[JobDataEntity] = {

    val query = "?q=direction:" + jobtypeTwoId

    val esRDD = sc.esJsonRDD("job_data/jdbc", query)

    val jobs = esRDD.map(x => {
      val json = JSON.parseObject(x._2)

      val id = json.getString("id")
      val direction = json.getString("direction")
      val jobName = json.getString("job_name")
      val companyName = json.getString("company_name")
      val jobSiteProvinces = json.getString("job_site_provinces")
      val jobSite = json.getString("job_site")
      val jobSalaryMin = json.getString("job_salary_min")
      val jobSalaryMax = json.getString("job_salary_max")
      val relaseDate = json.getString("relase_date")
      val educationLevel = json.getString("education_level")
      val workExper = json.getString("work_exper")
      val companyWelfare = json.getString("company_welfare")
      val jobRequire = json.getString("job_require")
      val companyType = json.getString("company_type")
      val companyPeopleNum = json.getString("company_people_num")
      val companyBusiness = json.getString("company_business")

      JobDataEntity(id, direction, jobName, companyName, jobSiteProvinces, jobSite, jobSalaryMin, jobSalaryMax, relaseDate, educationLevel,
        workExper, companyWelfare, jobRequire, companyType, companyPeopleNum, companyBusiness)
    })

    println("从es读取完毕")

    jobs
  }

  private def dataInFromElasticsearchToday(jobtypeTwoId: String): RDD[JobDataEntity] = {

    val query = "?q=direction:" + jobtypeTwoId

    val esRDD = sc.esJsonRDD("job_data_current/jdbc", query)

    val jobs = esRDD.map(x => {
      val json = JSON.parseObject(x._2)

      val id = json.getString("id")
      val direction = json.getString("direction")
      val jobName = json.getString("job_name")
      val companyName = json.getString("company_name")
      val jobSiteProvinces = json.getString("job_site_provinces")
      val jobSite = json.getString("job_site")
      val jobSalaryMin = json.getString("job_salary_min")
      val jobSalaryMax = json.getString("job_salary_max")
      val relaseDate = json.getString("relase_date")
      val educationLevel = json.getString("education_level")
      val workExper = json.getString("work_exper")
      val companyWelfare = json.getString("company_welfare")
      val jobRequire = json.getString("job_require")
      val companyType = json.getString("company_type")
      val companyPeopleNum = json.getString("company_people_num")
      val companyBusiness = json.getString("company_business")

      JobDataEntity(id, direction, jobName, companyName, jobSiteProvinces, jobSite, jobSalaryMin, jobSalaryMax, relaseDate, educationLevel,
        workExper, companyWelfare, jobRequire, companyType, companyPeopleNum, companyBusiness)
    })

    println("从es读取today完毕")

    jobs
  }


}
