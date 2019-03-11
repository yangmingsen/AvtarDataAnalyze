package spark.rdd.current

import java.util

import entity.{EducationJobNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD
import top.ccw.avtar.db.Update

/** *
  * 描述： 分析学历占比
  *
  * 分析完毕后存入 tb_analyze_education_jobnum 表
  *
  * @author yangminsen
  */
object EducationJobNumAnalyze {

  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    //拿到每个职位的学历
    val rdd1 = jobsRDD.filter(x => {
      (x.educationLevel != null) && (x.educationLevel.length != 0)
    }).map(x => {
      val level = x.educationLevel.replaceAll("  ", "")
      (level, 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _)
    val list = new util.ArrayList[EducationJobNumEntity]()

    rdd2.collect().toList.map(x => list.add(EducationJobNumEntity(x._1, x._2)))

    //print to Test
    println("EducationJobNumAnalyze = " + rdd2.collect().toBuffer)

    //do write to Databse
    Update.ToTbCurrentEducationJobnum(list)

  }
}
