package spark.rdd.current

import java.util

import com.google.common.base.CharMatcher
import entity.{EducationSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

/** *
  * 描述： 以时间为轴，分析学历薪资历史走向
  *
  * @author ljq
  */
object EducationSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （学历,最小薪资,最大薪资,发布时间）
      */
    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (x.educationLevel!= "")
    }).map(x => {
      val level = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
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

      (x._1, ave)
    })

    val list = new util.ArrayList[EducationSalaryAveEntity]()
    rdd3.collect().toList.map(x => list.add(entity.EducationSalaryAveEntity(x._1, x._2)))


    //print to Test
    println("EducationSalaryAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database


  }

  def isWeekRange1(date: String): Boolean = {
    val now = date.substring(8, 10).toInt
    now >= 1 && now <= 7
  }

  def isWeekRange2(date: String): Boolean = {
    val now = date.substring(8, 10).toInt
    now > 7 && now <= 14
  }

  def isWeekRange3(date: String): Boolean = {
    val now = date.substring(8, 10).toInt
    now > 14 && now <= 21
  }

  def isWeekRange4(date: String): Boolean = {
    val now = date.substring(8, 10).toInt
    now > 21 && now <= 28
  }
}
