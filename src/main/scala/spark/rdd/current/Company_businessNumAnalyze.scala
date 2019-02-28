package spark.rdd.current

import java.util
import entity.{Company_businessNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

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
    val list = new util.ArrayList[Company_businessNumEntity]()

    rdd2.collect().toList.map(x => list.add(Company_businessNumEntity(x._1, x._2)))

    //print to Test
    println("Company_businessNumAnalyze = " + rdd2.collect().toBuffer)

    //do write to Databse

  }
}
