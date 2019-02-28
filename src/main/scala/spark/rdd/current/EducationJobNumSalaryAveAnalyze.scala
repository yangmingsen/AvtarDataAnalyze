package spark.rdd.current

import java.util

import entity.{EducationJobNumSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

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
    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (x.educationLevel.replaceAll("\\s*", "") != "")
    }).map(x => {
      val level = x.educationLevel.replaceAll("\\s*", "")
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (level, ave)
    })

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

    val list = new util.ArrayList[EducationJobNumSalaryAveEntity]()
    rdd3.collect().toList.map(x => list.add(entity.EducationJobNumSalaryAveEntity(x._3, x._2, x._1)))


    //print to Test
    println("EducationJobNumSalaryAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database


  }
}
