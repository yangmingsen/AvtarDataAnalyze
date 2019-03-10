package top.ccw.avtar.entity;

/***
 *
 * 待清洗的数据实体
 *
 * update by:
 * date:
 *
 * created by yangmingsen
 * date: 2019/03/09
 *
 */
public class JobDataOne {
    private int direction; //方向id
    private String jobName;//职位名
    private String companyName;//公司名
    private String jobSite;//工作地点
    private String jobSalary;//工作薪资
    private String relaseDate;//发布时间
    private String jobInfo1;//岗位信息1
    private String companyWelfare;//公司福利
    private String jobInfo2;//岗位信息2
    private String companyType;//公司类型
    private String companyPeopleNum;//公司人数
    private String companyBusiness;//公司业务

    public JobDataOne(int direction, String jobName, String companyName,
                      String jobSite, String jobSalary, String relaseDate,
                      String jobInfo1, String companyWelfare, String jobInfo2,
                      String companyType, String companyPeopleNum, String companyBusiness) {

        this.direction = direction;
        this.jobName = jobName;
        this.companyName = companyName;
        this.jobSite = jobSite;
        this.jobSalary = jobSalary;
        this.relaseDate = relaseDate;
        this.jobInfo1 = jobInfo1;
        this.companyWelfare = companyWelfare;
        this.jobInfo2 = jobInfo2;
        this.companyType = companyType;
        this.companyPeopleNum = companyPeopleNum;
        this.companyBusiness = companyBusiness;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getJobSite() {
        return jobSite;
    }

    public void setJobSite(String jobSite) {
        this.jobSite = jobSite;
    }

    public String getJobSalary() {
        return jobSalary;
    }

    public void setJobSalary(String jobSalary) {
        this.jobSalary = jobSalary;
    }

    public String getRelaseDate() {
        return relaseDate;
    }

    public void setRelaseDate(String relaseDate) {
        this.relaseDate = relaseDate;
    }

    public String getJobInfo1() {
        return jobInfo1;
    }

    public void setJobInfo1(String jobInfo1) {
        this.jobInfo1 = jobInfo1;
    }

    public String getCompanyWelfare() {
        return companyWelfare;
    }

    public void setCompanyWelfare(String companyWelfare) {
        this.companyWelfare = companyWelfare;
    }

    public String getJobInfo2() {
        return jobInfo2;
    }

    public void setJobInfo2(String jobInfo2) {
        this.jobInfo2 = jobInfo2;
    }

    public String getCompanyType() {
        return companyType;
    }

    public void setCompanyType(String companyType) {
        this.companyType = companyType;
    }

    public String getCompanyPeopleNum() {
        return companyPeopleNum;
    }

    public void setCompanyPeopleNum(String companyPeopleNum) {
        this.companyPeopleNum = companyPeopleNum;
    }

    public String getCompanyBusiness() {
        return companyBusiness;
    }

    public void setCompanyBusiness(String companyBusiness) {
        this.companyBusiness = companyBusiness;
    }

    @Override
    public String toString() {
        return "JobDataOne{" +
                "direction=" + direction +
                ", jobName='" + jobName + '\'' +
                ", companyName='" + companyName + '\'' +
                ", jobSite='" + jobSite + '\'' +
                ", jobSalary='" + jobSalary + '\'' +
                ", relaseDate='" + relaseDate + '\'' +
                ", companyWelfare='" + companyWelfare + '\'' +
                ", companyType='" + companyType + '\'' +
                ", companyPeopleNum='" + companyPeopleNum + '\'' +
                ", companyBusiness='" + companyBusiness + '\'' +
                '}';
    }
}
