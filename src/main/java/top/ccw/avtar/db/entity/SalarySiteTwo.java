package top.ccw.avtar.db.entity;


public class SalarySiteTwo {
    private int id;
    private int jobtypeTwoId;
    private int coloumn;
    private String result;
    private String requireMax;
    private String salaryMax;
    private String date;


    public int getColoumn() {
        return coloumn;
    }

    public void setColoumn(int coloumn) {
        this.coloumn = coloumn;
    }

    public SalarySiteTwo(int id, int jobtypeTwoId, int coloumn, String result,
                         String requireMax, String salaryMax, String date) {
        this.id = id;
        this.jobtypeTwoId = jobtypeTwoId;
        this.coloumn = coloumn;
        this.result = result;
        this.requireMax = requireMax;
        this.salaryMax = salaryMax;
        this.date = date;
    }


    public String getRequireMax() {
        return requireMax;
    }

    public void setRequireMax(String requireMax) {
        this.requireMax = requireMax;
    }

    public String getSalaryMax() {
        return salaryMax;
    }

    public void setSalaryMax(String salaryMax) {
        this.salaryMax = salaryMax;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getJobtypeTwoId() {
        return jobtypeTwoId;
    }

    public void setJobtypeTwoId(int jobtypeTwoId) {
        this.jobtypeTwoId = jobtypeTwoId;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "SalarySiteTwo{" +
                "id=" + id +
                ", jobtypeTwoId=" + jobtypeTwoId +
                ", result='" + result + '\'' +
                '}';
    }
}
