package spark.rdd.statistical

import java.util

import com.google.common.base.CharMatcher
import entity.{EducationSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, dbutils}

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
    val data1 = List("01-3", "01-10", "01-17", "01-24", "01-31")
    val data2 = List("高中", "中专", "大专", "本科", "硕士", "博士")
    val data3 = List(0, 1, 2, 3, 4)

    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && x.educationLevel != "" && (CharMatcher.WHITESPACE.trimFrom(x.educationLevel) != "")
    }).map(x => {
      val level = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      val relaseDate = x.relaseDate
      ((level, relaseDate), ave)
    })

    val list1 = new util.ArrayList[Double]()
    for (y <- data2) {
      for (z <- data3) {
        val rdd2 = rdd1.filter(x => {
          isWeekRange(x._1._2) == z && x._1._1.equals(y)
        }).map(x => {
          ((x._1._1, "2019-" + data1(z)), x._2)
        }).cache()
        val rdd3 = rdd2.countByKey()
        val rdd4 = rdd2.reduceByKey(_ + _).map(x => {
          val num = rdd3.get(x._1) match {
            case Some(v) => v.toLong
            case None => 1
          }
          val ave_salary = (x._2 / num).formatted("%.2f").toDouble
          (x._1._1, ave_salary, "2019-" + data1(z))
        }).cache()
        if (rdd4.count() > 0) {
          rdd4.collect().toList.map(x => list1.add(x._2))
        }
        else {
          list1.add(0)
        }
      }
    }

    val list2 = list1.subList(0, 5)
    val list3 = list1.subList(5, 10)
    val list4 = list1.subList(10, 15)
    val list5 = list1.subList(15, 20)
    val list6 = list1.subList(20, 25)
    val list7 = list1.subList(25, 30)

    val list8 = new util.ArrayList[String]()
    for (x <- data1) {
      list8.add(x)
    }
    val list9 = new util.ArrayList[String]()
    for (x <- data2) {
      list9.add(x)
    }
    val list = new util.ArrayList[EducationSalaryAveEntity]()
    list.add(EducationSalaryAveEntity(list8, list9, list2, list3, list4, list5, list6, list7))

    //write to database
    val gsonStr = ConvertToJson.ToJson7(list)
    //println(gsonStr.substring(1, gsonStr.length() - 1))
    dbutils.update_statistical("tb_statistical_education_jobnum_salaryave",gsonStr.substring(1,gsonStr.length()-1))

  }

  def isWeekRange(date: String): Int = {
    val day = date.substring(8, date.length).toInt
    if (day <= 3)
      0
    else if (day > 3 & day <= 10)
      1
    else if (day > 10 && day <= 17)
      2
    else if (day > 17 && day <= 24)
      3
    else
      4
  }

}
