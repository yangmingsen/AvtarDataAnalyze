package utils;

import com.google.gson.Gson;
import entity.*;

import java.util.ArrayList;

/**
 * @program: AvtarDataAnalyze
 * @description:转json工具类
 * @author: ljq
 * @create: 2019-03-05 20:48
 **/

public class ConvertToJson {
    public static String ToJson1(ArrayList<tb_statistical_companybusiness_num> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }

    public static String ToJson2(ArrayList<tb_statistical_jobname_num> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }

    public static String ToJson3(ArrayList<tb_statistical_companytype_num> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }

    public static String ToJson4(ArrayList<SalaryWorkExperJobNumAve> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }

    public static String ToJson5(ArrayList<EducationJobNumSalaryAve> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }
    public static String ToJson6(ArrayList<EducationCompanyTypeJobNum> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }
    public static String ToJson7(ArrayList<EducationSalaryAveEntity> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }
    public static String ToJson8(ArrayList<CompanyTypeSalaryAveEntity> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }
    public static String ToJson9(ArrayList<Object> list) {
        Gson gson = new Gson();
        String gsonStr = gson.toJson(list);
        return gsonStr;
    }
}