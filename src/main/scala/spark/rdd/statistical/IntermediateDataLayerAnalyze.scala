package spark.rdd.statistical

import java.util

import com.google.common.base.CharMatcher
import entity.{IntermediateDataLayerEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import utils.ConvertToJson

/**
  * @author ljq
  * created on 2019-03-13 19:10
  **/
object IntermediateDataLayerAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    val rdd1 = jobsRDD.filter(x => {
      x.jobName.length != 0
    }).map(x => {
      val direction = x.direction
      /*val jobName = x.jobName*/
      (direction, 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = jobsRDD.filter(x => {
      x.jobName.length != 0
    }).map(x => {
      val jobName = x.jobName
      (jobName, 1)
    })
    val rdd4 = rdd3.reduceByKey(_ + _).sortBy(_._2, false).take(1)

    val rdd5 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (x.jobName.length != 0)
    }).map(x => {
      val direction = x.direction
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      (direction, ave)
    })
    val rdd7 = rdd5.countByKey()
    val rdd6 = rdd5.reduceByKey(_ + _).map(x => {
      val jobNum = rdd7.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble
      (x._1, jobNum, ave)
    })

    val rdd8 = jobsRDD.filter(x => {
      x.jobName.length != 0 && x.jobSite.length !=0
    }).map(x => {
      val jobSite = x.jobSite
      (jobSite, 1)
    })
    val rdd9 = rdd8.reduceByKey(_ + _).sortBy(_._2, false).take(1)

    val rdd10 = jobsRDD.filter(x => {
      x.jobName.length != 0 && x.educationLevel!="" && (CharMatcher.WHITESPACE.trimFrom(x.educationLevel) != "")
    }).map(x => {
      val educationLevel = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
      (educationLevel, 1)
    })
    val rdd11 = rdd10.reduceByKey(_ + _).sortBy(_._2, false).take(1)

    val rdd12 = jobsRDD.filter(x => {
      x.jobName.length != 0 && (x.companyType.length != 0)
    }).map(x => {
      val companyType = x.companyType
      (companyType, 1)
    })
    val rdd13 = rdd12.reduceByKey(_ + _).sortBy(_._2, false).take(1)

    val rdd14 = jobsRDD.filter(x => {
      x.jobName.length != 0 && (x.workExper != "")
    }).map(x => {
      val workExper = CharMatcher.WHITESPACE.trimFrom(x.workExper).substring(0, 1) match {
        case "无" => "无工作经验"
        case _ => CharMatcher.WHITESPACE.trimFrom(x.workExper)
      }
      (workExper, 1)
    })
    val rdd15 = rdd14.reduceByKey(_ + _).sortBy(_._2, false).take(1)

    val list = new util.ArrayList[Object]()
    rdd2.collect().toList.map(x => list.add("value1\":\""+x._2.toString))
    rdd4.foreach(x => list.add("value2\":\""+x._1))
    rdd6.collect().toList.map(x => list.add("value3\":\""+x._3))
    rdd9.foreach(x => list.add("value4\":\""+x._1))
    rdd11.foreach(x => list.add("value5\":\""+x._1))
    rdd13.foreach(x => list.add("value6\":\""+x._1))
    rdd15.foreach(x => list.add("value7\":\""+x._1))

    //do write to Databse
    val str = ConvertToJson.ToJson9(IntermediateDataLayerEntity(list))
    println(str)
    //dbutils.insert(str, "tb_statistical_companybusiness_num")
  }
}
