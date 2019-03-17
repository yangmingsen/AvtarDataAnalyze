package spark.rdd.statistical

import java.text.SimpleDateFormat
import java.util
import java.util.Date

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
      x.companyType != "" && x.companyPeopleNum != ""
    }).map(x => {
      val companyType = x.companyType
      val num = x.companyPeopleNum.toLong
      (companyType, num)
    })

    val rdd2 = rdd1.countByKey()

    val rdd3 = rdd1.reduceByKey(_ + _).sortBy(_._1, true).map(x => {
      val jobNum = rdd2.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = x._2 / jobNum

      (ave, x._1)
    })

    val list = new util.ArrayList[tb_statistical_companytype_num]()
    rdd3.collect().toList.map(x => list.add(entity.tb_statistical_companytype_num(x._1, x._2)))

    //write to database
    val str = ConvertToJson.ToJson3(list)

    //println(str)
    if (dbutils.judge_statistical("tb_statistical_companytype_num", getNowDate())) {
      dbutils.insert_statistical("tb_statistical_companytype_num", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_companytype_num", str)
  }

  def getNowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val nowdate = dateFormat.format(now)
    nowdate
  }
}
