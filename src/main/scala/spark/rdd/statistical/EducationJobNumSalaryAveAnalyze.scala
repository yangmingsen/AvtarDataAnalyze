package spark.rdd.statistical

import java.util

import com.google.common.base.CharMatcher
import entity.{EducationJobNumSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.dbutils

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
      (x.jobSalaryMin.length != 0) && (CharMatcher.WHITESPACE.trimFrom(x.educationLevel) != "")
    }).map(x => {
      val level = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (level, ave)
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

    val list = new util.ArrayList[EducationJobNumSalaryAveEntity]()
    rdd3.collect().toList.map(x => list.add(entity.EducationJobNumSalaryAveEntity(x._3, x._2, x._1)))
    rdd3.collect().toList.map(x => list1.add("'"+x._1+"'")&&list2.add(x._3))
    //print to Test
    //println("EducationJobNumSalaryAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database
    val list3 = "["+list1+","+list2+"]"

    dbutils.insert(list3, "tb_statistical_education_salaryave")
  }
}
