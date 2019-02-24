package spark.rdd.current

import java.util

import entity.{CompanyTypeJobNumSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

/***
  * 描述： 职位在全国范围内公司类型情况（平均薪资，职位数）的分布状况分析
  *
  * 分析完毕后存入 tb_current_companytype_jobnum 表
  *
  * @author yangminsen
  */
object CompanyTypeJobNumSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity]): Unit = {

    /***
      * 获取 （公司类型,最小薪资,最大薪资）
      */
    val rdd1 = jobsRDD.map(x => {
      val companyType = x.companyType
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble+max.toDouble)/2.0

      (companyType,ave)
    })


    val rdd2 = rdd1.countByKey()

    //
    val jobRDD3 = rdd1.reduceByKey(_ + _).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble

      (x._1,jobNum,ave)
    })

    val list = new util.ArrayList[CompanyTypeJobNumSalaryAveEntity]()
    jobRDD3.collect().toList.map(x => list.add(CompanyTypeJobNumSalaryAveEntity(x._1,x._2,x._3)))

    //write to database


  }
}
