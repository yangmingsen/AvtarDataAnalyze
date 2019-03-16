package spark.rdd.current

import java.util

import entity.{CompanyTypeJobNumSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import top.ccw.avtar.db.Update

/***
  * 描述： 职位在全国范围内公司类型情况（平均薪资，职位数）的分布状况分析
  *
  * 分析完毕后存入 tb_current_companytype_jobnum 表
  *
  * @author yangminsen
  */
object CompanyTypeJobNumSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity],jobtypeTwoId: String): Unit = {

    /***
      * 获取 （公司类型,最小薪资,最大薪资）
      */
    val rdd1 = jobsRDD.filter(x =>{(x.jobSalaryMin != "None") && (x.companyType.length!=0)}).map(x => {
      val companyType = x.companyType.replaceAll("\\s*", "")
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble+max.toDouble)/2.0

      (companyType,ave)
    })

    val rdd2 = rdd1.countByKey()

    //
    val rdd3 = rdd1.reduceByKey(_ + _).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble

      (x._1,jobNum,ave)
    })

    val list = new util.ArrayList[CompanyTypeJobNumSalaryAveEntity]()
    rdd3.collect().toList.map(x => list.add(CompanyTypeJobNumSalaryAveEntity(x._1,x._2,x._3)))


    //print to Test
    println("CompanyTypeJobNumSalaryAveAnalyze = "+rdd3.collect().toBuffer)

    //write to database
    //Update.ToTbCurrentCompanytypeJobnum(list)

  }
}
