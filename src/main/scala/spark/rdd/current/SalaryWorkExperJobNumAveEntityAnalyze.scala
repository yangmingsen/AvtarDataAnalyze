package spark.rdd.current

import entity.{JobDataEntity, SalaryWorkExperJobNumAveEntity}
import org.apache.spark.rdd.RDD
import java.util

import com.google.common.base.CharMatcher

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
      (x.jobSalaryMin.length != 0) && (x.workExper != "")
    }).map(x => {
      val workExper = CharMatcher.WHITESPACE.trimFrom(x.workExper).substring(0, 1) match {
        case "无" => "0"
        case _ => CharMatcher.WHITESPACE.trimFrom(x.workExper).substring(0, 1)
      }
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (workExper, ave)
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

    val list = new util.ArrayList[SalaryWorkExperJobNumAveEntity]()
    rdd3.collect().toList.map(x => list.add(entity.SalaryWorkExperJobNumAveEntity(x._1, x._2, x._3)))


    //print to Test
    println("SalaryWorkExperJobNumAveEntityAnalyze = " + rdd3.collect().toBuffer)

    //write to database


  }

}
