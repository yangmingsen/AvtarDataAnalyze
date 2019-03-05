package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, JobNameNumEntity}
import org.apache.spark.rdd.RDD

/** *
  * 描述： 通过统计技术计算各个职位名的个数，展示出目前比较火热的职位名
  *
  * @author ljq
  */
object JobNameNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    val rdd1 = jobsRDD.filter(x => {
      (x.jobName != null) && (x.jobName.length != 0)
    }).map(x => {
      val jobName = x.jobName.replaceAll("\\s*", "")
      (jobName, 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).sortBy(_._2, false)
    val list = new util.ArrayList[JobNameNumEntity]()

    rdd2.collect().toList.map(x => list.add(JobNameNumEntity(x._1, x._2)))

    //print to Test
    println("JobNameNumAnalyze = " + rdd2.collect().toBuffer)

    //do write to Databse

  }

}
