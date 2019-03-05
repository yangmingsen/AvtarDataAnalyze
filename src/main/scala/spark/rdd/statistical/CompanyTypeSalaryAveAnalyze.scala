package spark.rdd.statistical

import java.util

import entity.{CompanyTypeSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

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
    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (x.companyType != "")
    }).map(x => {
      val companyType = x.companyType
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0

      (companyType, ave)
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

    val list = new util.ArrayList[CompanyTypeSalaryAveEntity]()
    rdd3.collect().toList.map(x => list.add(entity.CompanyTypeSalaryAveEntity(x._1, x._2)))


    //print to Test
    println("CompanyTypeSalaryAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database


  }

}
