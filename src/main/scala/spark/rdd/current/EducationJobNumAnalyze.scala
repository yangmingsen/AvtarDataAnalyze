package spark.rdd.current

import java.util

import entity.{EducationJobNumEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

/***
  * 描述： 分析学历占比
  *
  * 分析完毕后存入 tb_analyze_education_jobnum 表
  *
  * @author yangminsen
  */
object EducationJobNumAnalyze {

  def start(jobsRDD: RDD[JobDataEntity]): Unit = {
    //拿到每个职位的学历
    val rdd1 = jobsRDD.map(x => {
      val level = x.educationLevel.replaceAll("  ","")
      (level,1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)
    val list = new util.ArrayList[EducationJobNumEntity]()

    rdd2.collect().toList.map(x => list.add(EducationJobNumEntity(x._1,x._2)))

    //do write to Databse

  }
}
