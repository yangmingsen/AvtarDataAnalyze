package top.ccw.avtar.db.entity;

public class TimeSalaryOne {
    private int id;
    private int jobtypeTwoId;
    private int column;
    private String time;
    private String result;
    private String forecast;

    public TimeSalaryOne(int id, int jobtypeTwoId, int column, String time, String result, String forecast) {
        this.id = id;
        this.jobtypeTwoId = jobtypeTwoId;
        this.column = column;
        this.time = time;
        this.result = result;
        this.forecast = forecast;
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

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getForecast() {
        return forecast;
    }

    public void setForecast(String forecast) {
        this.forecast = forecast;
    }

    @Override
    public String toString() {
        return "TimeSalaryOne{" +
                "id=" + id +
                ", jobtypeTwoId=" + jobtypeTwoId +
                ", time='" + time + '\'' +
                ", result='" + result + '\'' +
                ", forecast='" + forecast + '\'' +
                '}';
    }
}
