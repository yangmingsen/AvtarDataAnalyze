package spark.rdd.current

import java.util

import entity.{JobDataEntity, TimeSalaryEntity}
import org.apache.spark.rdd.RDD

/** *
  * 描述： 分析薪资与时间的关系
  * 分析完毕后存入 tb_analyze_time_salary 表
  *
  * @author yangminsen
  */
object TimeSalaryAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取每个时间对应的平均薪资
      */
    val rdd1 = jobsRDD.filter(x => x.jobSalaryMin!="").map(x => {
      val date = x.relaseDate.substring(5, x.relaseDate.length)
      val salary_min = x.jobSalaryMin.toDouble
      val salary_max = x.jobSalaryMax.toDouble

      val ave = (salary_min + salary_max) / 2.0

      (date, ave)
    })

    //根据key对对每个value进行相加. 也就是根据时间对每个职位的平均薪资进行相加
    val rdd2 = rdd1.reduceByKey(_ + _)

    //统计key出现次数. 也就是每个时间出现的次数
    val rdd3 = rdd1.countByKey()

    //?分析多少天的数据
    val rdd4 = rdd2.map(x => {
      val value = rdd3.get(x._1) match {
        case Some(v) => v.toDouble
        case _ => 1.0
      }
      val ave = (x._2 / value).formatted("%.2f")

      (x._1, ave)
    })

    val rdd5 = rdd4.sortByKey(true)

    val list = new util.ArrayList[TimeSalaryEntity]()
    rdd5.collect().toList.map(x => list.add(TimeSalaryEntity(x._1, x._2)))

    //print to Test
    println("TimeSalaryAnalyze = " + rdd5.collect().toBuffer)

    //do write database

    //Update.ToTbCurrentTimeSalary(list)
  }
}
