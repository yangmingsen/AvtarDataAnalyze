package spark.rdd.statistical

import java.util

import entity.{EducationCompanyTypeJobNum, EducationCompanyTypeJobNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

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
    val rdd1 = jobsRDD.repartition(10).filter(x => {
      x.educationLevel != "" && x.companyType != ""
    }).mapPartitions(it => it.map(x => {
      val level = x.educationLevel
      val companyType = x.companyType
      ((companyType, level), 1)
    })).cache()
    val rdd2 = rdd1.countByKey()

    val rdd3 = rdd1.reduceByKey(_ + _, 10).sortBy(_._1, false).mapPartitions(it => it.map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      (x._1._1, jobNum, x._1._2)
    })).cache()

    val list = new util.ArrayList[EducationCompanyTypeJobNumEntity]()
    rdd3.collect().map(x => list.add(entity.EducationCompanyTypeJobNumEntity(x._1, x._2, x._3)))

    val data1 = List("高中", "中专", "大专", "本科", "硕士", "博士")
    val data2 = List("国企", "上市公司", "创业公司", "外资（非欧美）", "外资（欧美）", "民营公司", "合资", "事业单位")
    val list1 = new util.ArrayList[Long]()

    for (y <- data1) {
      for (z <- data2) {
        val rdd = rdd3.filter(x => {
          x._3.equals(y) && x._1.equals(z)
        }).cache()
        if (rdd.count() > 0) {
          rdd.collect().map(x => list1.add(x._2))
        }
        else {
          list1.add(0)
        }
      }
    }

    val list2 = list1.subList(0, 8)
    val list3 = list1.subList(8, 16)
    val list4 = list1.subList(16, 24)
    val list5 = list1.subList(24, 32)
    val list6 = list1.subList(32, 40)
    val list7 = list1.subList(40, 48)

    val list8 = new util.ArrayList[String]()
    for (x <- data2) {
      list8.add(x)
    }
    val list9 = new util.ArrayList[EducationCompanyTypeJobNum]()
    list9.add(EducationCompanyTypeJobNum(list8, list2, list3, list4, list5, list6, list7))

    //do write database
    val gsonStr = ConvertToJson.ToJson6(list9)
    val str = gsonStr.substring(1, gsonStr.length() - 1)
    //println(str)

    if (dbutils.judge_statistical("tb_statistical_education_companytype_jobnum", TimeUtils.getNowDate(), jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_education_companytype_jobnum", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_education_companytype_jobnum", str, TimeUtils.getNowDate(), jobtypeTwoId)
  }
}
