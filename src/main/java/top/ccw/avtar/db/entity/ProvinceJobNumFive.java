package top.ccw.avtar.db.entity;


public class ProvinceJobNumFive {
    private int id;
    private int jobtypeTwoId;
    private int column;
    private String result;
    private String date;
    private long dayNum;
    private long weekNum;

    public ProvinceJobNumFive(int id, int jobtypeTwoId, int column,String result, String date, long dayNum, long weekNum) {
        this.id = id;
        this.jobtypeTwoId = jobtypeTwoId;
        this.column = column;
        this.result = result;
        this.date = date;
        this.dayNum = dayNum;
        this.weekNum = weekNum;
    }

    @Override
    public String toString() {
        return "ProvinceJobNumFive{" +
                "id=" + id +
                ", jobtypeTwoId=" + jobtypeTwoId +
                ", result='" + result + '\'' +
                ", date='" + date + '\'' +
                ", dayNum=" + dayNum +
                ", weekNum=" + weekNum +
                '}';
    }

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
    }

    public long getDayNum() {
        return dayNum;
    }

    public void setDayNum(long dayNum) {
        this.dayNum = dayNum;
    }

    public long getWeekNum() {
        return weekNum;
    }

    public void setWeekNum(long weekNum) {
        this.weekNum = weekNum;
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
