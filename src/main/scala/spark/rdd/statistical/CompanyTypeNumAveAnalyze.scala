package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, tb_statistical_companytype_num}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, dbutils}

/** *
  * 描述： 按公司类型统计，得到各个公司类型的平均人数
  *
  * @author ljq
  */
object CompanyTypeNumAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （公司类型,人数）
      */
    val rdd1 = jobsRDD.filter(x => {
      (x.companyType.length != 0) && (x.companyPeopleNum.length != 0)
    }).map(x => {
      val companyType = x.companyType
      val num = x.companyPeopleNum.replaceAll("少于", "").replaceAll("人", "")
      if (num.contains("-")) {
        val num1 = num.split("-", 2)(0).toDouble
        (companyType, num1)
      }
      else {
        val num1 = num.toDouble
        (companyType, num1)
      }
    })

    val rdd2 = rdd1.countByKey()

    //
    val rdd3 = rdd1.reduceByKey(_ + _).sortBy(_._1, true).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.0f").toInt

      (ave, x._1)
    })

    val list = new util.ArrayList[tb_statistical_companytype_num]()
    rdd3.collect().toList.map(x => list.add(entity.tb_statistical_companytype_num(x._1, x._2)))


    //print to Test
    //println("CompanyTypeNumAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database
    val str = ConvertToJson.ToJson3(list)
    dbutils.update_statistical("tb_statistical_companytype_num", str)
  }

}
