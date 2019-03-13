package entity

import java.util

/** *
  *
  * @param date   日期
  * @param salary 当日的平均薪资
  */
case class TimeSalaryEntity(val date: String, val salary: String)


/** *
  *
  * @param site   地区
  * @param salary 地区平均薪资
  * @param jobNum 地区职位数
  */
case class SalarySiteEntity(val site: String, val salary: Double, val jobNum: Long)


/** *
  *
  * @param level 学历
  * @param num   学历数
  */
case class EducationJobNumEntity(val level: String, val num: Long)


/** *
  *
  * @param jobName     职位名
  * @param salary      职位平均薪资
  * @param site        职位地点
  * @param companyName 公司名称
  */
case class JobNameRankEntity(val jobName: String, val salary: Double, val site: String, val companyName: String)

/** *
  * 这里的地区为 市
  *
  * @param province 地区名字 (本来是省，后面改为地区-市。然后这个字段就没有更改)
  * @param num      地区职位数
  */
case class ProvinceJobNumEntity(val province: String, val num: Long)


/** *
  *
  * @param companyType 公司类型
  * @param jobNum      公司类型的职位数
  * @param salary      公司类型的平均薪资
  */
case class CompanyTypeJobNumSalaryAveEntity(val companyType: String, val jobNum: Long, val salary: Double)


/** *
  * 这是展现出当前方向的职位公司都是属于那些业务的公司的词云统计
  *
  * @param company_business 公司业务
  * @param num              对应的公司业务的词频
  */
case class Company_businessNumEntity(val company_business: String, val num: Long)

case class tb_statistical_companybusiness_num(name: String, value: Long)

/** *
  *
  * @param workExper 工作经验
  * @param salary    对应的平均薪资
  * @param jobNum    对应的职位数
  */
case class SalaryWorkExperJobNumAveEntity(val workExper: String, val jobNum: Long, val salary: Double)

case class SalaryWorkExperJobNumAve(val workExper: util.ArrayList[String], val ave_salary: util.ArrayList[Double])

/** *
  *
  * @param companyType 公司类型
  * @param level       学历
  * @param jobNum      对应的职位数
  */
case class EducationCompanyTypeJobNumEntity(val companyType: String, val jobNum: Long, val level: String)

case class EducationCompanyTypeJobNum(val companyType: util.ArrayList[String], val highSchool: util.List[Long], val secondarySchool: util.List[Long], val college: util.List[Long], val bachelor: util.List[Long], val masterDegree: util.List[Long], val doctor: util.List[Long])

/** *
  *
  * @param level  学历
  * @param salary 对应的平均薪资
  * @param jobNum 对应的职位数
  */
case class EducationJobNumSalaryAveEntity(val salary: Double, val jobNum: Long, val level: String)

case class EducationJobNumSalaryAve(val education: util.ArrayList[String], val ave_salary: util.ArrayList[Double])

/** *
  *
  * @param CompanyType 公司类型
  * @param Num         对应的平均人数
  */
case class CompanyTypeNumAveEntity(val Num: Int, val CompanyType: String)

case class tb_statistical_companytype_num(value: Long, name: String)

/** *
  *
  * @param jobName 职位名
  * @param Num     对应的人数
  */
case class JobNameNumEntity(val jobName: String, val Num: Long)

case class tb_statistical_jobname_num(name: String, value: Long)

/** *
  *
  * @param relaseDate 日期
  */
case class EducationSalaryAveEntity(val relaseDate: util.ArrayList[String],  val education: util.ArrayList[String],val highSchool: util.List[Double], val secondarySchool: util.List[Double], val college: util.List[Double], val bachelor: util.List[Double], val masterDegree: util.List[Double], val doctor: util.List[Double])

case class CompanyTypeSalaryAveEntity(val source: util.ArrayList[Object])

case class IntermediateDataLayerEntity(val data: util.ArrayList[Object])

case class JobDataEntity(direction: String, jobName: String, companyName: String, jobSiteProvinces: String, jobSite: String, jobSalaryMin: String,
                         jobSalaryMax: String, relaseDate: String, educationLevel: String, workExper: String, companyWelfare: String,
                         jobResp: String, jobRequire: String, companyType: String, companyPeopleNum: String, companyBusiness: String)

class Entity {

}