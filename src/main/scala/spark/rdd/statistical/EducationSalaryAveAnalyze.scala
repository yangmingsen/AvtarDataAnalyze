package spark.rdd.statistical

import java.util

import com.google.common.base.CharMatcher
import entity.{EducationSalaryAveEntity, JobDataEntity}
import org.apache.spark.rdd.RDD

/** *
  * 描述： 以时间为轴，分析学历薪资历史走向
  *
  * @author ljq
  */
object EducationSalaryAveAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    val data1 = List("01-3", "01-10", "01-17", "01-24", "01-31")
    val data2 = List("高中", "中专", "大专", "本科", "硕士", "博士")
    /** *
      * 获取 （学历,最小薪资,最大薪资,发布时间）
      */
    val rdd1 = jobsRDD.filter(x => {
      (x.jobSalaryMin.length != 0) && (CharMatcher.WHITESPACE.trimFrom(x.educationLevel) != "")
    }).map(x => {
      val level = CharMatcher.WHITESPACE.trimFrom(x.educationLevel)
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      val relaseDate = x.relaseDate
      ((level, relaseDate), ave)
      /*if(relaseDate<=data1(0))
        ((level,"01-3"), ave)
      else if(relaseDate>data1(0)&&relaseDate<=data1(1))
        ((level,"01-10"), ave)
      else if(relaseDate>data1(1)&&relaseDate<=data1(2))
        ((level,"01-17"), ave)
      else if(relaseDate>data1(2)&&relaseDate<=data1(3))
        ((level,"01-24"), ave)
      else if(relaseDate>data1(3)&&relaseDate<=data1(4))
        ((level,"01-31"), ave)*/
    })

    for (y <- data2) {
      val rdd2 = rdd1.filter(x => {
        x._1._2 <= data1(0) && x._1._1.equals(y)
      })
      val rdd3 = rdd2.reduceByKey(_ + _).map(x => {
        val num = rdd2.count()
        val ave_salary = (x._2 / num).formatted("%.2f").toDouble
        (x._1._1, ave_salary, data1(0))
      })
      println(rdd3.collect().toBuffer)
    }

    val list1 = new util.ArrayList[Double]()
    val list2 = new util.ArrayList[Double]()
    val list3 = new util.ArrayList[Double]()
    val list4 = new util.ArrayList[Double]()
    val list5 = new util.ArrayList[Double]()


    /* }*/
    val list6 = new util.ArrayList[String]()
    for (x <- data1) {
      list6.add(x)
    }
    val list = new util.ArrayList[EducationSalaryAveEntity]()
    /*list.add(list3,)*/
    //rdd3.collect().toList.map(x => list.add(entity.EducationSalaryAveEntity(x._1, x._2)))


    //print to Test
    //println("EducationSalaryAveAnalyze = " + rdd3.collect().toBuffer)

    //write to database


  }

}
