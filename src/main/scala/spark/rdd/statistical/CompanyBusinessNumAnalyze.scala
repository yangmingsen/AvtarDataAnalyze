package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, tb_statistical_companybusiness_num}
import org.apache.spark.rdd.RDD
import utils.ConvertToJson

/** *
  * 描述： 分析相同公司业务的词频
  *
  * @author ljq
  */
object CompanyBusinessNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    val rdd1 = jobsRDD.filter(x => {
      x.companyBusiness != ""
    }).map(x => {
      val business = x.companyBusiness
      (business, 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).sortBy(_._2, false)
    val list = new util.ArrayList[tb_statistical_companybusiness_num]()

    rdd2.collect().toList.map(x => list.add(tb_statistical_companybusiness_num(x._1, x._2)))

    //do write to Databse
    val str = ConvertToJson.ToJson1(list)
    //println(str)
    //dbutils.update_statistical("tb_statistical_companybusiness_num", str)
  }
}
