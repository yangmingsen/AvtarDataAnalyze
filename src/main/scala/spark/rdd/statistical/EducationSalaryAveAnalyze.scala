package spark.rdd.statistical

import java.util

import entity.{EducationSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

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
    val data1 = List("02-18", "02-25", "03-02", "03-09", "03-15")
    val data2 = List("高中", "中专", "大专", "本科", "硕士", "博士")
    val data3 = List(0, 1, 2, 3, 4)

    val rdd1 = jobsRDD.repartition(10).filter(x => {
      x.jobSalaryMin != "" && x.educationLevel != ""
    }).mapPartitions(it => it.map(x => {
      val level = x.educationLevel
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      val relaseDate = x.relaseDate
      ((level, relaseDate), ave)
    })).cache()

    val list1 = new util.ArrayList[Double]()
    for (y <- data2) {
      for (z <- data3) {
        val rdd2 = rdd1.filter(x => {
          TimeUtils.isWeekRange(x._1._2) == z && x._1._1.equals(y)
        }).mapPartitions(it => it.map(x => {
          ((x._1._1, "2019-" + data1(z)), x._2)
        })).cache()
        val rdd3 = rdd2.countByKey()
        val rdd4 = rdd2.reduceByKey(_ + _, 10).mapPartitions(it => it.map(x => {
          val num = rdd3.get(x._1) match {
            case Some(v) => v.toLong
            case None => 1
          }
          val ave_salary = (x._2 / num).formatted("%.2f").toDouble
          (x._1._1, ave_salary, "2019-" + data1(z))
        })).cache()
        if (rdd4.count() > 0) {
          rdd4.collect().map(x => list1.add(x._2))
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
    val str = gsonStr.substring(1, gsonStr.length() - 1)
    //println(str)
    if (dbutils.judge_statistical("tb_statistical_education_jobnum_salaryave", TimeUtils.getNowDate(), jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_education_jobnum_salaryave", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_education_jobnum_salaryave", str, TimeUtils.getNowDate(), jobtypeTwoId)
  }
}