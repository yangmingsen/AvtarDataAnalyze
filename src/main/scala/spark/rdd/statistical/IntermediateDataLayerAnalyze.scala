package spark.rdd.statistical

import java.util

import entity.{IntermediateDataLayerEntity, IntermediateDataLayerEntity1, JobDataEntity}
import org.ansj.library.DicLibrary
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.rdd.RDD
import utils.{ConvertToJson, TimeUtils, dbutils}

import scala.io.Source

/**
  * @author ljq
  *         Created on 2019-03-13 19:10
  **/
object IntermediateDataLayerAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {

    val rdd0 = jobsRDD.repartition(10)

    val rdd = rdd0.filter(x => {
      x.jobName != ""
    }).cache()

    val rdd1 = rdd.mapPartitions(it => it.map(x => {
      val direction = x.direction
      (direction, 1)
    })).cache()
    val rdd2 = rdd1.reduceByKey(_ + _, 10).cache()

    val rdd3 = rdd.mapPartitions(it => it.map(x => {
      val jobName = x.jobName
      (jobName, 1)
    })).cache()
    val rdd4 = rdd3.reduceByKey(_ + _, 10).sortBy(_._2, false).take(1)

    val rdd5 = rdd0.filter(x => {
      x.jobSalaryMin != "" && x.jobName != ""
    }).mapPartitions(it => it.map(x => {
      val direction = x.direction
      val min = x.jobSalaryMin.toDouble
      val max = x.jobSalaryMax.toDouble
      val ave = (min.toDouble + max.toDouble) / 2.0
      (direction, ave)
    })).cache()
    val rdd7 = rdd5.countByKey()
    val rdd6 = rdd5.reduceByKey(_ + _, 10).mapPartitions(it => it.map(x => {
      val jobNum = rdd7.get(x._1) match {
        case Some(v) => v.toLong
        case None => 1
      }
      val ave = (x._2 / jobNum).formatted("%.2f").toDouble
      (x._1, ave)
    })).cache()

    val rdd8 = rdd0.filter(x => {
      x.jobName != "" && x.jobSite != ""
    }).mapPartitions(it => it.map(x => {
      val jobSite = x.jobSite
      (jobSite, 1)
    })).cache()
    val rdd9 = rdd8.reduceByKey(_ + _, 10).sortBy(_._2, false).take(1)

    val rdd10 = rdd0.filter(x => {
      x.jobName != "" && x.educationLevel != ""
    }).mapPartitions(it => it.map(x => {
      val educationLevel = x.educationLevel
      (educationLevel, 1)
    })).cache()
    val rdd11 = rdd10.reduceByKey(_ + _, 10).sortBy(_._2, false).take(1)

    val rdd12 = rdd0.filter(x => {
      x.jobName != "" && x.companyType != ""
    }).mapPartitions(it => it.map(x => {
      val companyType = x.companyType
      (companyType, 1)
    })).cache()
    val rdd13 = rdd12.reduceByKey(_ + _, 10).sortBy(_._2, false).take(1)

    val rdd14 = rdd0.filter(x => {
      x.jobName != "" && x.workExper != ""
    }).mapPartitions(it => it.map(x => {
      val workExper = x.workExper
      (workExper, 1)
    })).cache()
    val rdd15 = rdd14.reduceByKey(_ + _, 10).sortBy(_._2, false).take(1)

    val list0 = new util.ArrayList[IntermediateDataLayerEntity]()
    val list = new util.ArrayList[IntermediateDataLayerEntity1]()

    val list1 = new util.ArrayList[String]()
    list1.add("当前职位总数")
    rdd2.collect().map(x => list1.add(x._2.toString))
    val list2 = new util.ArrayList[String]()
    list2.add("最热门职位")
    rdd4.foreach(x => list2.add(x._1))
    val list3 = new util.ArrayList[String]()
    list3.add("平均薪资")
    rdd6.collect().map(x => list3.add(x._2.toString))
    val list4 = new util.ArrayList[String]()
    list4.add("需求最大城市")
    rdd9.foreach(x => list4.add(x._1))
    val list5 = new util.ArrayList[String]()
    list5.add("需求最大的学历要求")
    rdd11.foreach(x => list5.add(x._1))
    val list6 = new util.ArrayList[String]()
    list6.add("需求最大的公司类型")
    rdd13.foreach(x => list6.add(x._1))
    val list7 = new util.ArrayList[String]()
    list7.add("需求最大的工作经验要求")
    rdd15.foreach(x => list7.add(x._1))

    val data = rdd0.filter(x => {
      x.jobRequire != ""
    }).mapPartitions(it => it.map(x => x.jobRequire))

    val data1 = new util.ArrayList[String]()
    for (word <- Source.fromFile("src/main/scala/spark/rdd/ParticipleText/ability").getLines()) {
      word.split(",").foreach(x => data1.add(x))
    }

    val data2 = new util.ArrayList[String]()
    for (word <- Source.fromFile("src/main/scala/spark/rdd/ParticipleText/technology1").getLines()) {
      word.split(",").foreach(x => data2.add(x))
    }
    //添加自定义词典
    val dicfile = raw"src/main/scala/spark/rdd/ParticipleText/ExtendDic" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
    //逐行读入文本文件，将其添加到自定义词典中
    for (word <- Source.fromFile(dicfile).getLines) {
      DicLibrary.insert(DicLibrary.DEFAULT, word)
    }

    //添加停用词词典
    val stopworddicfile = raw"src/main/scala/spark/rdd/ParticipleText/StopWordDic" //stopworddicfile为一个文本文件的名字，里面每一行存放一个词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", null) //过滤掉标点
    filter.insertStopRegexes("^[0-9]*$", "\\s*") //过滤掉数字
    for (word <- Source.fromFile(stopworddicfile).getLines) {
      filter.insertStopWords(word)
    }

    val splited = data.map(x => DicAnalysis.parse(x.replaceAll("\\s*", "")).recognition(filter).toStringWithOutNature(" ")).cache()

    val wordcloud = splited.flatMap(_.split(" ")).mapPartitions(it => it.map((_, 1))).reduceByKey(_ + _, 10).sortBy(_._2, false).cache().take(250)

    val list8 = new util.ArrayList[String]()
    list8.add("需求最大的能力要求")
    wordcloud.filter(x => x._1 != "" && data1.contains(x._1)).take(1).foreach(x => list8.add(x._1))
    val list9 = new util.ArrayList[String]()
    list9.add("最热门技术")
    wordcloud.filter(x => x._1 != "" && data2.contains(x._1.toLowerCase())).take(1).foreach(x => list9.add(x._1))

    //do write to Databse
    list.add(IntermediateDataLayerEntity1(list1, list2, list3, list4, list5, list6, list7, list8, list9))
    list0.add(IntermediateDataLayerEntity(list))
    val gsonstr = ConvertToJson.ToJson9(list0)
    val str = gsonstr.substring(1, gsonstr.length() - 1)
    //println(str)
    if (dbutils.judge_statistical("tb_statistical_mediatedatalayer", TimeUtils.getNowDate(), jobtypeTwoId)) {
      dbutils.insert_statistical("tb_statistical_mediatedatalayer", str, jobtypeTwoId)
    }
    else
      dbutils.update_statistical("tb_statistical_mediatedatalayer", str, TimeUtils.getNowDate(), jobtypeTwoId)
  }

}