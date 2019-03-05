package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, tb_statistical_companybusiness_num}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, dbutils}

/** *
  * 描述： 分析相同公司业务的词频
  *
  * @author ljq
  */
object Company_businessNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    val rdd1 = jobsRDD.filter(x => {
      (x.companyBusiness != null) && (x.companyBusiness.length != 0)
    }).map(x => {
      val business = x.companyBusiness.replaceAll("\\s*", "")
      (business, 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).sortBy(_._2, false)
    val list = new util.ArrayList[tb_statistical_companybusiness_num]()

    rdd2.collect().toList.map(x => list.add(tb_statistical_companybusiness_num(x._1, x._2)))

    //print to Test
    //println("Company_businessNumAnalyze = " + rdd2.collect().toBuffer)

    //do write to Databse
    val str = ConvertToJson.ToJson1(list)
    dbutils.insert(str, "tb_statistical_companybusiness_num")
  }
}
