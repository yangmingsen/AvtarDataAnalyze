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
    val rdd = jobsRDD.filter(x=>{x.id.toInt<100}).repartition(10).mapPartitions(it=>it.map(x => {
      val hbaseDomain = new HbaseDomain(x.id, x.direction, x.jobName, x.companyName, x.jobSiteProvinces, x.jobSite, x.jobSalaryMin, x.jobSalaryMax, x.relaseDate, x.educationLevel, x.workExper, x.companyWelfare, x.jobRequire, x.companyType, x.companyPeopleNum, x.companyBusiness)
      hbaseDomainList.append(hbaseDomain)
    }))
    hbaseDomainList
  }

  def getHbaseDomainArray(jobsRDD: RDD[JobDataEntity]): ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    val domains = getHbaseDomain(jobsRDD)
    val array = new ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]
    domains.foreach(domain => {
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info1"), Bytes.toBytes("direction"), Bytes.toBytes(domain.direction)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info1"), Bytes.toBytes("jobName"), Bytes.toBytes(domain.jobName)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info1"), Bytes.toBytes("companyName"), Bytes.toBytes(domain.companyName)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info1"), Bytes.toBytes("jobSiteProvinces"), Bytes.toBytes(domain.jobSiteProvinces)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info1"), Bytes.toBytes("jobSite"), Bytes.toBytes(domain.jobSite)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info2"), Bytes.toBytes("jobSalaryMin"), Bytes.toBytes(domain.jobSalaryMin)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info2"), Bytes.toBytes("jobSalaryMax"), Bytes.toBytes(domain.jobSalaryMax)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info2"), Bytes.toBytes("relaseDate"), Bytes.toBytes(domain.relaseDate)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info2"), Bytes.toBytes("educationLevel"), Bytes.toBytes(domain.educationLevel)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info2"), Bytes.toBytes("workExper"), Bytes.toBytes(domain.workExper)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info3"), Bytes.toBytes("companyWelfare"), Bytes.toBytes(domain.companyWelfare)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info3"), Bytes.toBytes("jobRequire"), Bytes.toBytes(domain.jobRequire)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info3"), Bytes.toBytes("companyType"), Bytes.toBytes(domain.companyType)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info3"), Bytes.toBytes("companyPeopleNum"), Bytes.toBytes(domain.companyPeopleNum)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info3"), Bytes.toBytes("companyBusiness"), Bytes.toBytes(domain.companyBusiness)))))
    })
    array
  }
}
