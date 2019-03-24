package spark.rdd.current

import java.util

import entity.{CityJobNumEntity, JobDataEntity, ProvinceJobNumEntity}
import org.apache.spark.rdd.RDD
import top.ccw.avtar.db.Update
import top.ccw.avtar.utils.DateHelper


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
      val jobSite = x.jobSiteProvinces//省
      val relaseDate = x.relaseDate
      val jobCity = x.jobSite //市
      (jobSite, relaseDate,jobCity)
    })

    //按省地区统计职位数，并封装为RDD[ProvinceJobNumEntity()]
    val rdd2 = rdd1.filter(x => {
      !x._1.equals("异地")
    }).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => ProvinceJobNumEntity(x._1, x._2.toLong))

    //这里按市
    val RDD2 = rdd1.filter(x => {x._3 != "异地招聘"}).map(x => (x._3,1))
      .reduceByKey(_ + _).map(x =>CityJobNumEntity(x._1,x._2))

    //获取当天日期
    val day = DateHelper.getYYYY_MM_DD

    //统计当前职位数据 该天职位数
    val jobDayNum = rdd1.filter(x => (x._2 == day)).count()
    //统计当前职位数据 日期范围职位数
    val jobWeekNum = rdd1.filter(x => {
      isWeekRange(x._2)
    }).count()

    val list = new util.ArrayList[ProvinceJobNumEntity]()
    rdd2.collect().toList.map(x => list.add(x))

    val list2 = new util.ArrayList[CityJobNumEntity]()
    RDD2.collect().toList.map(x => list2.add(x))

    //print to Test
    //println("ProvinceJobNumAnalyze = " + rdd2.collect().toBuffer)

    //do write database
    Update.ToTbCurrentProvinceJobnum(list, jobDayNum, jobWeekNum,list2)


  }

  def isWeekRange(date: String): Boolean = {

    val nowDate = DateHelper.getYYYY_MM_DD.substring(8,10).toInt

    val matchDate = date.substring(8, 10).toInt

    matchDate > nowDate-7 && matchDate < nowDate
  }
}
