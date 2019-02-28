package spark.rdd.current

import java.util

import entity.{EducationCompanyTypeJobNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

/** *
  * 描述： 按公司类型分类，统计出每种公司类型的学历数的关系分析
  *
  * @author ljq
  */
object EducationCompanyTypeJobNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （公司类型,学历)
      */
    val rdd1 = jobsRDD.filter(x => {
      (x.educationLevel.length != 0) && (x.companyType.replaceAll("\\s*", "") != "")
    }).map(x => {
      val level = x.educationLevel.replaceAll("\\s*", "")
      val companyType = x.companyType.replaceAll("\\s*", "")
      ((companyType, level), 1)
    })
    val rdd2 = rdd1.countByKey()

    val rdd3 = rdd1.reduceByKey(_ + _).sortBy(_._1, false).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      (x._1._1, jobNum, x._1._2)
    })

    val list = new util.ArrayList[EducationCompanyTypeJobNumEntity]()
    rdd3.collect().toList.map(x => list.add(entity.EducationCompanyTypeJobNumEntity(x._1, x._2, x._3)))

    //print to Test
    println("EducationCompanyTypeJobNumAnalyze = " + rdd3.collect().toBuffer)

    //do write database

  }
}
