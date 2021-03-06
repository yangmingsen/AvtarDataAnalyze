package spark.rdd.current

import java.util

import entity.{JobDataEntity, SalarySiteEntity}
import org.apache.spark.rdd.RDD
import top.ccw.avtar.db.Update

/** *
  * 描述： 分析地区薪资的关系
  * 分析完毕后存入 tb_analyze_salary_site 表
  *
  * @author yangminsen
  */
object SalarySiteAnalyze {

  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取每个地区对应的平均薪资
      */
    val rdd1 = jobsRDD.filter(x => {(x.jobSalaryMin != "") && (!x.jobSite.equals("异地招聘"))}).map(x => {
      val site = x.jobSite
      val salary_min = x.jobSalaryMin.toDouble
      val salary_max = x.jobSalaryMax.toDouble

      val ave = (salary_min + salary_max) / 2.0

      (site, ave)
    })

    val rdd2 = rdd1.reduceByKey(_ + _) //各地区的平均薪资总和 按地区

    val rdd3 = rdd1.countByKey() //各地区职位数统计 按地区

    //得到（地区，平均薪资，职位数）
    val rdd4 = rdd2.map(x => {
      val jobNum = rdd3.get(x._1) match {
        case Some(h) => h.toDouble
        case _ => 1.0
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble
      (x._1, ave, jobNum.toLong)
    })

    //按平均薪资排序
    val rdd5 = rdd4.sortBy(x => x._2, false)

    //职位需求量最大的城市
    val requireMax = rdd5.sortBy(x => x._3,false).collect().toList.head._1

    //平均薪资水平最高的城市
    val salaryMax = rdd5.collect().head._1


    val list = new util.ArrayList[SalarySiteEntity]()
    rdd5.collect().toList.take(8).map(x => list.add(SalarySiteEntity(x._1, x._2, x._3)))

    //print to Test
    println("SalarySiteAnalyze = " + rdd5.collect().toBuffer)

    //do write dabase
    Update.ToTbCurrentSalarySite(list,requireMax,salaryMax)

  }
}
