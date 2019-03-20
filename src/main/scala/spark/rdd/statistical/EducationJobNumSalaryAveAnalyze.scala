package spark.rdd.statistical

import java.util

import entity.{EducationJobNumSalaryAve, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

/** *
  * 描述：按学历统计每个职位的平均薪资的关系分析
  *
  * @author ljq
  */
object EducationJobNumSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （学历,最小薪资,最大薪资）
      */
    val rdd1 = jobsRDD.repartition(10).filter(x => {
      x.jobSalaryMin != "" && x.educationLevel != ""
    }).mapPartitions(it => it.map(x => {
      val level = x.educationLevel
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (level, ave)
    })).cache()

    val list1 = new util.ArrayList[String]()
    val list2 = new util.ArrayList[Double]()
    val rdd2 = rdd1.countByKey()

    val rdd3 = rdd1.reduceByKey(_ + _, 10).sortBy(_._1, true).mapPartitions(it => it.map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble
      (x._1, jobNum, ave)
    })).cache()

    //val list = new util.ArrayList[EducationJobNumSalaryAveEntity]()
    val list3 = new util.ArrayList[EducationJobNumSalaryAve]()
    //rdd3.collect().toList.map(x => list.add(entity.EducationJobNumSalaryAveEntity(x._3, x._2, x._1)))
    rdd3.collect().map(x => list1.add(x._1) && list2.add(x._3))
    list3.add(EducationJobNumSalaryAve(list1, list2))

    //write to database
    val gsonStr = ConvertToJson.ToJson5(list3)
    val str = gsonStr.substring(1, gsonStr.length() - 1)
    //println(str)
    if (dbutils.judge_statistical("tb_statistical_education_salaryave", TimeUtils.getNowDate(), jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_education_salaryave", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_education_salaryave", str, TimeUtils.getNowDate(), jobtypeTwoId)
  }
}