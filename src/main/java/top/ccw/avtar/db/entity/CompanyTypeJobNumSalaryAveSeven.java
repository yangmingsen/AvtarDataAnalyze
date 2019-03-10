package top.ccw.avtar.db.entity;


public class CompanyTypeJobNumSalaryAveSeven {

    private int id;
    private int jobtypeTwoId;
    private int column;
    private String result;
    private String date;

    public CompanyTypeJobNumSalaryAveSeven(int id, int jobtypeTwoId, int column, String result, String date) {
        this.id = id;
        this.jobtypeTwoId = jobtypeTwoId;
        this.column = column;
        this.result = result;
        this.date = date;
    }

    public CompanyTypeJobNumSalaryAveSeven(int jobtypeTwoId, int column, String result, String date) {
        this.jobtypeTwoId = jobtypeTwoId;
        this.column = column;
        this.result = result;
        this.date = date;
    }

    @Override
    public String toString() {
        return "CompanyTypeJobNumSix{" +
                "id=" + id +
                ", jobtypeTwoId=" + jobtypeTwoId +
                ", column=" + column +
                ", result='" + result + '\'' +
                ", date='" + date + '\'' +
                '}';
    }

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
