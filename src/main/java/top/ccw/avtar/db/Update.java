package top.ccw.avtar.db;

import com.google.gson.Gson;
import entity.*;
import top.ccw.avtar.db.dao.*;
import top.ccw.avtar.db.entity.*;


import java.util.List;

/***
 *描述： 插入分析结果
 * <p>流程：</p>
 *      <p>1、首先根据当前时间和方向id，去数据库查询。</p>
 *      <p>2、如果不存在记录，则执行插入操作</p>
 *      <p>3、如果存在记录。获取对应id, 根据id更新记录</p>
 *
 * @author yangmingsen
 */
public class Update {
    private static int jobtypeTwoId = 0;
    private static String  time = "";

    private static int getJobtypeTwoId() {
        return jobtypeTwoId;
    }

    public static void setUpdateInfo(int jobtypeTwoId, String time) {
        Update.jobtypeTwoId = jobtypeTwoId;
        Update.time = time;
    }

    private static String getTime() {
        return time;
    }



    public static void ToTbCurrentCompanytypeJobnum(List<CompanyTypeJobNumSalaryAveEntity> list) {

        Integer id = CompanyTypeJobNumSalaryAveSevenDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);

        if (id != 0) {
            CompanyTypeJobNumSalaryAveSevenDao.getInstance().
                    update(new CompanyTypeJobNumSalaryAveSeven(id,jobtypeTwoId,0,gsonStr,time));
        } else {
            CompanyTypeJobNumSalaryAveSevenDao.getInstance().
                    insert(new CompanyTypeJobNumSalaryAveSeven(jobtypeTwoId,0,gsonStr,time));
        }


    }

    public static void ToTbCurrentEducationJobnum(List<EducationJobNumEntity> list) {

        Integer id = EducationJobNumThreeDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);

        if(id != 0) {
            EducationJobNumThreeDao.getInstance().
                    update(new EducationJobNumThree(id,jobtypeTwoId,gsonStr,time));
        } else  {
            EducationJobNumThreeDao.getInstance().
                    insert(new EducationJobNumThree(jobtypeTwoId,gsonStr,0,time));
        }

    }

    public static void ToTbCurrentJobnameRank(List<JobNameRankEntity> list) {

        Integer id = JobNameRankFourDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);

        if(id != 0) {
            JobNameRankFourDao.getInstance().
                    update(new JobNameRankFour(id,jobtypeTwoId,0,gsonStr,time));
        } else  {
            JobNameRankFourDao.getInstance().
                    insert(new JobNameRankFour(0,jobtypeTwoId,0,gsonStr,time));
        }

    }

    public static void ToTbCurrentProvinceJobnum(List<ProvinceJobNumEntity> list, long dayNum, long weekNum) {

        Integer id = ProvinceJobNumFiveDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);

        if(id != 0) {
            ProvinceJobNumFiveDao.getInstance().
                    update(new ProvinceJobNumFive(id,jobtypeTwoId,0,gsonStr,time,dayNum,weekNum));
        } else  {
            ProvinceJobNumFiveDao.getInstance().
                    insert(new ProvinceJobNumFive(0,jobtypeTwoId,0,gsonStr,time,dayNum,weekNum));
        }

    }

    public static void ToTbCurrentSalarySite(List<SalarySiteEntity> list,
                                             String requireMax, String salaryMax) {
        Integer id = SalarySiteTwoDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);

        if(id != 0) {
            SalarySiteTwoDao.getInstance().
                    update(new SalarySiteTwo(id,jobtypeTwoId,0,gsonStr,requireMax,salaryMax,time));
        } else  {
            SalarySiteTwoDao.getInstance().
                    insert(new SalarySiteTwo(0,jobtypeTwoId,0,gsonStr,requireMax,salaryMax,time));
        }

    }

    public static void ToTbCurrentTimeSalary(List<TimeSalaryEntity> list) {

        Integer id = TimeSalaryOneDao.getInstance().searchId(jobtypeTwoId,time);

        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        String forecast = "";

        if(id != 0) {
            TimeSalaryOneDao.getInstance().
                    update(new TimeSalaryOne(id,jobtypeTwoId,0,time,gsonStr,forecast));
        } else  {
            TimeSalaryOneDao.getInstance().
                    insert(new TimeSalaryOne(0,jobtypeTwoId,0,time,gsonStr,forecast));
        }

    }





}
