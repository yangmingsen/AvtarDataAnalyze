package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, SalaryWorkExperJobNumAve}
import org.apache.spark.rdd.RDD
import utils.ConvertToJson

/** *
  * 描述： 职位当前方向工作经验与薪资（平均薪资，职位数）的关系分析
  *
  * @author ljq
  */
object SalaryWorkExperJobNumAveEntityAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （工作经验--按最小值来,最小薪资,最大薪资）
      */
    val rdd1 = jobsRDD.filter(x => {
      x.jobSalaryMin.length != 0 && x.workExper != ""
    }).map(x => {
      val workExper = x.workExper
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (workExper, ave)
    })

    val list1 = new util.ArrayList[String]()
    val list2 = new util.ArrayList[Double]()

    val rdd2 = rdd1.countByKey()

    //
    val rdd3 = rdd1.reduceByKey(_ + _).sortBy(_._1, true).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble

      (x._1, jobNum, ave)
    })

    //val list = new util.ArrayList[SalaryWorkExperJobNumAveEntity]()
    val list3 = new util.ArrayList[SalaryWorkExperJobNumAve]()
    //rdd3.collect().toList.map(x => list.add(entity.SalaryWorkExperJobNumAveEntity(x._1, x._2, x._3)))
    rdd3.collect().toList.map(x => list1.add(x._1) && list2.add(x._3))
    list3.add(SalaryWorkExperJobNumAve(list1, list2))

    //write to database
    val gsonStr = ConvertToJson.ToJson4(list3)
    println(gsonStr.substring(1,gsonStr.length()-1))
    //dbutils.update_statistical("tb_statistical_salary_workexper_jobnumave", gsonStr.substring(1, gsonStr.length() - 1))

  }

}
