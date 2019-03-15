package spark.rdd.statistical

import java.util

import entity.{CompanyTypeSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.ConvertToJson

/** *
  * 描述： 以时间为轴、分析公司类型薪资历史走向
  *
  * @author ljq
  */
object CompanyTypeSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    /** *
      * 获取 （公司类型,最小薪资,最大薪资,发布时间）
      */
    val data1 = List("01-3", "01-10", "01-17", "01-24", "01-31")
    val data2 = List("国企", "上市公司", "创业公司", "外资（非欧美）", "外资（欧美）", "民营公司", "合资", "事业单位")
    val data3 = List(0, 1, 2, 3, 4)

    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (x.companyType != "")
    }).map(x => {
      val companyType = x.companyType
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      val relaseDate = x.relaseDate

      ((companyType, relaseDate), ave)
    })

    val list1 = new util.ArrayList[Object]()
    for (y <- data2) {
      list1.add(y)
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
          rdd4.collect().toList.map(x => list1.add(x._2.toString))
        }
        else {
          list1.add("0")
        }
      }
    }

    val list2 = new util.ArrayList[Object]()
    val list3 = new util.ArrayList[Object]()
    val list4 = new util.ArrayList[Object]()
    val list5 = new util.ArrayList[Object]()
    val list6 = new util.ArrayList[Object]()
    val list7 = new util.ArrayList[Object]()
    val list8 = new util.ArrayList[Object]()
    val list9 = new util.ArrayList[Object]()

    for (i <- 0 until list1.size()) {
      if (0 <= i && i < 6)
        list2.add(list1.get(i))
      else if (6 <= i && i < 12)
        list3.add(list1.get(i))
      else if (12 <= i && i < 18)
        list4.add(list1.get(i))
      else if (18 <= i && i < 24)
        list5.add(list1.get(i))
      else if (24 <= i && i < 30)
        list6.add(list1.get(i))
      else if (30 <= i && i < 36)
        list7.add(list1.get(i))
      else if (36 <= i && i < 42)
        list8.add(list1.get(i))
      else
        list9.add(list1.get(i))
    }


    val list10 = new util.ArrayList[String]()
    list10.add("date")
    for (x <- data1) {
      list10.add(x)
    }
    val list11 = new util.ArrayList[Object]()
    list11.add(list10)
    list11.add(list2)
    list11.add(list3)
    list11.add(list4)
    list11.add(list5)
    list11.add(list6)
    list11.add(list7)
    list11.add(list8)
    list11.add(list9)
    val list = new util.ArrayList[CompanyTypeSalaryAveEntity]()
    list.add(CompanyTypeSalaryAveEntity(list11))

    //write to database
    val gsonStr = ConvertToJson.ToJson8(list)
    //println(gsonStr.substring(1, gsonStr.length() - 1))
    //dbutils.update_statistical("tb_statistical_companytype_salary", gsonStr.substring(1, gsonStr.length() - 1))

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
