package hbase.DataTransmission

import entity.{HbaseDomain, JobDataEntity}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created By ljq On 2019-03-18 13:43
  **/
object MysqlToHBase {

  def getHbaseDomain(jobsRDD: RDD[JobDataEntity]): ListBuffer[HbaseDomain] = {
    val hbaseDomainList = new ListBuffer[HbaseDomain]
    val rdd = jobsRDD.map(x => {
      val hbaseDomain = new HbaseDomain(x.id,x.direction,x.jobName,x.companyName,x.jobSiteProvinces,x.jobSite,x.jobSalaryMin,x.jobSalaryMax,x.relaseDate,x.educationLevel,x.workExper,x.companyWelfare,x.jobRequire,x.companyType,x.companyPeopleNum,x.companyBusiness)
      hbaseDomainList.append(hbaseDomain)
    })
    hbaseDomainList
  }

  def getHbaseDomainArray(jobsRDD: RDD[JobDataEntity]): ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    val domains = getHbaseDomain(jobsRDD)
    val array = new ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]
    domains.foreach(domain => {
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info"), Bytes.toBytes("direction"), Bytes.toBytes(domain.direction)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info"), Bytes.toBytes("jobName"), Bytes.toBytes(domain.jobName)))))
    })
    array
  }
}
