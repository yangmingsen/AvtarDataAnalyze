package spark.rdd.statistical

import java.util

import entity.{JobDataEntity, tb_statistical_jobname_num}
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

/** *
  * 描述： 通过统计技术计算各个职位名的个数，展示出目前比较火热的职位名
  *
  * @author ljq
  */
object JobNameNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    val rdd1 = jobsRDD.filter(x => {
      x.jobName != ""&&x.jobName.length<=5
    }).map(x => {
      val jobName = x.jobName
      (jobName, 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).sortBy(_._2, false)
    val list = new util.ArrayList[tb_statistical_jobname_num]()

    rdd2.collect().take(50).toList.map(x => list.add(tb_statistical_jobname_num(x._1, x._2)))

    //do write to Databse
    val str = ConvertToJson.ToJson2(list)
    //println(str)
    if (dbutils.judge_statistical("tb_statistical_jobname_num", TimeUtils.getNowDate(),jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_jobname_num", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_jobname_num", str,TimeUtils.getNowDate(),jobtypeTwoId)
  }
}
