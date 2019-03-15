package spark.streaming

import com.alibaba.fastjson.JSON
import entity.JobDataEntity
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.LoggerLevels

/***
  * 监听
  */
object KafkaInput {

  val zkQuorum = "yms1:2181,yms2:2181,yms3:2181"
  val group = "c1"
  val topics = "sortware"
  val numThreads = 2

  def dataStream(): Unit = {

    //控制台不显示日志信息
    LoggerLevels.setStreamingLogLevels()

    //设置任务名字，并指定启动线程为2
    val sparkConf = new SparkConf().setAppName("KafkaInput").setMaster("local[2]")

    //创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("c://ck2")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    println("topicMap = "+topicMap)

    //与kafka 建立连接
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val jobs = data.map(x => x._2).map(x => parseJobDataEntity(x))


    /***
      * 在使用Streaming和kafka结合 的时候需要触发如下方法 print否则出现如下的错误
      * <p>
      *   java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute
      * at scala.Predef$.require(Predef.scala:233)
      * at org.apache.spark.streaming.DStreamGraph.validate(DStreamGraph.scala:161)
      * at org.apache.spark.streaming.StreamingContext.validate(StreamingContext.scala:542)
      * at org.apache.spark.streaming.StreamingContext.liftedTree1$1(StreamingContext.scala:601)
      * at org.apache.spark.streaming.StreamingContext.start(StreamingContext.scala:600)
      * at org.bianqi.spark.day5.KafkaWordCount$.main(KafkaWordCount.scala:24)
      * at org.bianqi.spark.day5.KafkaWordCount.main(KafkaWordCount.scala)
      * Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute
      * at scala.Predef$.require(Predef.scala:233)
      * </p>
      */
    jobs.print()

    ssc.start()
    ssc.awaitTermination()
  }


  /***
    * 该方法为解析 objStr 为 JobDataEntity对象，采用com.alibaba.fastjson.JSON 工具
    * @author yangmingsen
    * @param objStr
    * @return
    */
  def parseJobDataEntity(objStr: String): JobDataEntity = {

    val jsonObj = JSON.parseObject(objStr)

    val direction = jsonObj.get("direction").toString
    val jobName = jsonObj.get("jobName").toString
    val companyName = jsonObj.get("companyName").toString


    val jobSiteProvinces = jsonObj.get("jobSiteProvinces").toString
    val jobSite = jsonObj.get("jobSite").toString
    val jobSalaryMin = jsonObj.get("jobSalaryMin").toString
    val jobSalaryMax = jsonObj.get("jobSalaryMax").toString


    val relaseDate = jsonObj.get("relaseDate").toString
    val educationLevel = jsonObj.get("educationLevel").toString
    val workExper = jsonObj.get("workExper").toString

    val companyWelfare = jsonObj.get("companyWelfare").toString
    val jobResp = jsonObj.get("jobResp").toString
    val jobRequire = jsonObj.get("jobRequire").toString

    val companyType = jsonObj.get("companyType").toString
    val companyPeopleNum = jsonObj.get("companyPeopleNum").toString
    val companyBusiness = jsonObj.get("companyBusiness").toString


    JobDataEntity(direction,jobName,companyName,jobSiteProvinces,jobSite,jobSalaryMin,jobSalaryMax,
      relaseDate,educationLevel,workExper,companyWelfare,jobRequire,
      companyType,companyPeopleNum,companyBusiness )
  }


  /***
    * 输出数据到HBase
    * @param jobs 数据流
    */
  def outStreamToHBase(jobs: DStream[JobDataEntity]): Unit = {

  }


  /***
    * 输出数据到MySQL
    * @param jobs 数据流
    */
  def outStreamToMySQL(jobs: DStream[JobDataEntity]): Unit = {

  }

}
