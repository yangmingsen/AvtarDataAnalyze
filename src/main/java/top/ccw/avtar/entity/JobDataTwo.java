package top.ccw.avtar.entity;

public class JobDataTwo {
    private long id;
    private int direction;
    private String jobName;
    private String companyName;
    private String jobSite;
    private String jobSalaryMin;
    private String jobSalaryMax;
    private String relaseDate;
    private String educationLevel;
    private String workExper;
    private String companyWelfare;
    private String jobRequire;
    private String companyType;
    private String companyPeopleNum;
    private String companyBusiness;

    public JobDataTwo() {
        super();
    }
    public JobDataTwo( int direction, String jobName, String companyName, String jobSite,
                          String jobSalaryMin, String jobSalaryMax, String relaseDate,
                          String educationLevel, String workExper,
                          String companyWelfare, String jobRequire,
                          String companyType,String companyPeopleNum,String companyBusiness) {
        super();
        this.direction = direction;
        this.jobName = jobName;
        this.companyName = companyName;
        this.jobSite = jobSite;
        this.jobSalaryMin = jobSalaryMin;
        this.jobSalaryMax = jobSalaryMax;
        this.relaseDate = relaseDate;
        this.educationLevel = educationLevel;
        this.workExper = workExper;
        this.companyWelfare = companyWelfare;
        this.jobRequire = jobRequire;
        this.companyType = companyType;
        this.companyPeopleNum = companyPeopleNum;
        this.companyBusiness = companyBusiness;
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
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
    public String getJobSalaryMin() {
        return jobSalaryMin;
    }
    public void setJobSalaryMin(String jobSalaryMin) {
        this.jobSalaryMin = jobSalaryMin;
    }
    public String getJobSalaryMax() {
        return jobSalaryMax;
    }
    public void setJobSalaryMax(String jobSalaryMax) {
        this.jobSalaryMax = jobSalaryMax;
    }
    public String getRelaseDate() {
        return relaseDate;
    }
    public void setRelaseDate(String relaseDate) {
        this.relaseDate = relaseDate;
    }
    public String getEducationLevel() {
        return educationLevel;
    }
    public void setEducationLevel(String educationLevel) {
        this.educationLevel = educationLevel;
    }
    public String getWorkExper() {
        return workExper;
    }
    public void setWorkExper(String workExper) {
        this.workExper = workExper;
    }
    public String getCompanyWelfare() {
        return companyWelfare;
    }
    public void setCompanyWelfare(String companyWelfare) {
        this.companyWelfare = companyWelfare;
    }
    public String getJobRequire() {
        return jobRequire;
    }
    public void setJobRequire(String jobRequire) {
        this.jobRequire = jobRequire;
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
        return "JobDataTwo{" +
                "id=" + id +
                ", direction=" + direction +
                ", jobName='" + jobName + '\'' +
                ", companyName='" + companyName + '\'' +
                ", jobSite='" + jobSite + '\'' +
                ", jobSalaryMin='" + jobSalaryMin + '\'' +
                ", jobSalaryMax='" + jobSalaryMax + '\'' +
                ", relaseDate='" + relaseDate + '\'' +
                ", educationLevel='" + educationLevel + '\'' +
                ", workExper='" + workExper + '\'' +
                ", companyWelfare='" + companyWelfare + '\'' +
                ", jobRequire='" + jobRequire + '\'' +
                ", companyType='" + companyType + '\'' +
                ", companyPeopleNum='" + companyPeopleNum + '\'' +
                ", companyBusiness='" + companyBusiness + '\'' +
                '}';
    }
}
