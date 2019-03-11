package top.ccw.avtar.db.entity;



public class EducationJobNumThree {
    private int id;
    private int jobtypeTwoId;
    private String result;
    private int column;
    private String date;

    public EducationJobNumThree(int id, int jobtypeTwoId, String result, String date) {
        this.id = id;
        this.jobtypeTwoId = jobtypeTwoId;
        this.result = result;
        this.date = date;
    }

    public EducationJobNumThree(int jobtypeTwoId, String result, int column, String date) {
        this.jobtypeTwoId = jobtypeTwoId;
        this.result = result;
        this.column = column;
        this.date = date;
    }

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "EducationJobNumThree{" +
                "id=" + id +
                ", jobtypeTwoId=" + jobtypeTwoId +
                ", result='" + result + '\'' +
                ", date='" + date + '\'' +
                '}';
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
