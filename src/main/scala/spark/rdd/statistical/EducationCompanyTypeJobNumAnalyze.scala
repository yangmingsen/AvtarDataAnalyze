package spark.rdd.statistical

import java.util

import com.google.common.base.CharMatcher
import entity.{EducationCompanyTypeJobNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.dbutils

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
      (x.educationLevel.length != 0) && (CharMatcher.WHITESPACE.trimFrom(x.companyType) != "")
    }).map(x => {
      val level = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
      val companyType = x.companyType
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
    val set = new util.HashSet[String]()
    rdd3.collect().toList.map(x => set.add("'" + x._1 + "'"))
    val data1 = List("高中", "中专", "大专", "本科", "硕士", "博士")
    val data2 = List("国企", "上市公司", "创业公司", "外资（非欧美）", "外资（欧美）", "民营公司", "合资", "事业单位")
    val list1 = new util.ArrayList[String]()

    for (y <- data1) {
      for (z <- data2) {
        val rdd = rdd3.filter(x => {
          x._3.equals(y) && x._1.equals(z)
        }).cache()
        if (rdd.count() > 0) {
          rdd.collect().toList.map(x => list1.add(x._2.toString))
        }
        else {
          list1.add("0")
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
      list8.add("'" + x + "'")
    }
    val list9 = "[" + list8 + ",[" + list2 + "," + list3 + "," + list4 + "," + list5 + "," + list6 + "," + list7 + "]]"
    //print to Test
    //println("EducationCompanyTypeJobNumAnalyze = " + rdd3.collect().toBuffer)

    //do write database
    dbutils.insert(list9, "tb_statistical_education_companytype_jobnum")
  }
}
