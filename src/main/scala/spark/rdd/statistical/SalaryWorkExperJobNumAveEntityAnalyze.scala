package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, SalaryWorkExperJobNumAve}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

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
    val rdd1 = jobsRDD.repartition(10).filter(x => {
      x.jobSalaryMin != "" && x.workExper != ""
    }).mapPartitions(it => it.map(x => {
      val workExper = x.workExper
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      (workExper, ave)
    })).cache()

    val list1 = new util.ArrayList[String]()
    val list2 = new util.ArrayList[Double]()

    val rdd2 = rdd1.countByKey()

    //
    val rdd3 = rdd1.reduceByKey(_ + _, 10).sortBy(_._1, true).mapPartitions(it => it.map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble

      (x._1, ave)
    })).cache()

    //val list = new util.ArrayList[SalaryWorkExperJobNumAveEntity]()
    val list3 = new util.ArrayList[SalaryWorkExperJobNumAve]()
    //rdd3.collect().toList.map(x => list.add(entity.SalaryWorkExperJobNumAveEntity(x._1, x._2, x._3)))
    rdd3.collect().map(x => list1.add(x._1) && list2.add(x._2))
    list3.add(SalaryWorkExperJobNumAve(list1, list2))

    //write to database
    val gsonStr = ConvertToJson.ToJson4(list3)
    val str = gsonStr.substring(1, gsonStr.length() - 1)
    //println(str)
    if (dbutils.judge_statistical("tb_statistical_salary_workexper_jobnumave", TimeUtils.getNowDate(), jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_salary_workexper_jobnumave", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_salary_workexper_jobnumave", str, TimeUtils.getNowDate(), jobtypeTwoId)
  }

}