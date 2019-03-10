package spark.rdd.current

import java.util

import entity.{JobDataEntity, ProvinceJobNumEntity}
import org.apache.spark.rdd.RDD
import top.ccw.avtar.db.Update


/** *
  * 描述： 当前方向地图
  *
  * 分析完毕后存入 tb_analyze_province_jobnum 表
  *
  * @author yangminsen
  */
object ProvinceJobNumAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    /** *
      * 获取 （职位地点,发布时间)
      */
    val rdd1 = jobsRDD.map(x => {
      val jobSite = x.jobSite
      val relaseDate = x.relaseDate
      (jobSite, relaseDate)
    })

    //按地区统计职位数，并封装为RDD[ProvinceJobNumEntity()]
    val rdd2 = rdd1.filter(x => {
      !x._1.equals("异地招聘")
    }).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => ProvinceJobNumEntity(x._1, x._2.toLong))

    val day = "2019-01-17"
    //统计当前职位数据 该天职位数
    val jobDayNum = rdd1.filter(x => (x._2 == day)).count()
    //统计当前职位数据 日期范围职位数
    val jobWeekNum = rdd1.filter(x => {
      isWeekRange(x._2)
    }).count()

    val list = new util.ArrayList[ProvinceJobNumEntity]()
    rdd2.collect().toList.map(x => list.add(x))

    //print to Test
    println("ProvinceJobNumAnalyze = " + rdd2.collect().toBuffer)

    //do write database
    Update.ToTbCurrentProvinceJobnum(list,jobDayNum,jobWeekNum)

  }

  def isWeekRange(date: String): Boolean = {
    val now = date.substring(8, 10).toInt
    now > 10 && now < 17
  }
}
