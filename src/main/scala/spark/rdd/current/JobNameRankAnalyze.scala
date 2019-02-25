package spark.rdd.current

import java.util

import entity.{JobDataEntity, JobNameRankEntity}
import org.apache.spark.rdd.RDD

/***
  * 描述： 分析全国当前方向的职位排名(按薪资)
  *
  * 分析完毕后存入 tb_analyze_jobname_rank 表
  *
  * @author yangminsen
  */
object JobNameRankAnalyze {

  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    //获取 (职位名，最小薪资，最大薪资，地点)
    val rdd1 = jobsRDD.filter(x => {(x.jobSalaryMin.length!=0)&&(x.jobSite.length!=0)}).map(x => {
      val name = x.jobName
      val min =  x.jobSalaryMin.toDouble
      val max =  x.jobSalaryMax.toDouble
      val site = x.jobSite
      val companyName = x.companyName

      val ave = (min+max)/2.0
      (name,ave,site,companyName)
    })

    val rdd2 = rdd1.sortBy(x => x._2,false)
    val list = new util.ArrayList[JobNameRankEntity]()

    rdd2.collect().toList.map(x => {list.add(JobNameRankEntity(x._1,x._2,x._3,x._4))})


    //print to Test
    println("JobNameRankAnalyze = "+rdd2.collect().toBuffer)

    //do write database

  }
}
