package spark.rdd.current

import java.util

import com.google.common.base.CharMatcher
import entity.{JobDataEntity, tb_analyze_job_requirements, tb_analyze_professional_skill}
import org.ansj.library.DicLibrary
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.rdd.RDD
import utils.ConvertToJson

import scala.io.Source

/**
  * @author ljq
  *         Created on 2019-03-17 14:23
  **/
object WordCloudAnalyze {
  def start(jobsRDD: RDD[JobDataEntity], jobtypeTwoId: String): Unit = {
    val data = jobsRDD.map(x => x.jobRequire)

    val data1 = new util.ArrayList[String]()
    for (word <- Source.fromFile(raw"../ParticipleText/ability", "GBK").getLines()) {
      word.split(",").foreach(x => data1.add(x))
    }

    val data2 = new util.ArrayList[String]()
    for (word <- Source.fromFile(raw"../ParticipleText/technology", "GBK").getLines()) {
      word.split(",").foreach(x => data2.add(x.toLowerCase()))
    }
    //添加自定义词典
    val dicfile = raw"../ParticipleText/ExtendDic" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
    //逐行读入文本文件，将其添加到自定义词典中
    for (word <- Source.fromFile(dicfile).getLines) {
      DicLibrary.insert(DicLibrary.DEFAULT, word)
    }

    //添加停用词词典
    val stopworddicfile = raw"../ParticipleText/StopWordDic" //stopworddicfile为一个文本文件的名字，里面每一行存放一个词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", null) //过滤掉标点
    filter.insertStopRegexes("^[0-9]*$", "\\s*", " ") //过滤掉数字和空字符
    for (word <- Source.fromFile(stopworddicfile).getLines) {
      filter.insertStopWords(word)
    }

    val splited = data.filter(_ != null).map(x => DicAnalysis.parse(CharMatcher.WHITESPACE.trimFrom(x.toString)).recognition(filter).toStringWithOutNature(" "))

    val wordcloud = splited.cache().flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).take(1000)

    val list1 = new util.ArrayList[tb_analyze_professional_skill]()
    val list2 = new util.ArrayList[tb_analyze_job_requirements]()

    wordcloud.filter(x => x._1 != " " && data1.contains(x._1)).take(50).foreach(x => list2.add(tb_analyze_job_requirements(x._1, x._2)))
    wordcloud.filter(x => x._1 != " " && data2.contains(x._1.toLowerCase())).take(50).foreach(x => list1.add(tb_analyze_professional_skill(x._1, x._2)))

    val str1 = ConvertToJson.ToJson10(list1)
    val str2 = ConvertToJson.ToJson11(list2)
    println(str1 + "\n" + str2)

    //dbutils.update_statistical("tb_current_professional_skill",str1)
    //dbutils.update_statistical("tb_current_job_requirements",str2)
  }
}
