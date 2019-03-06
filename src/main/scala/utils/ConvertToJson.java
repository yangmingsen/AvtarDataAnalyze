package utils;

import com.google.gson.Gson;
import entity.tb_statistical_companybusiness_num;
import entity.tb_statistical_jobname_num;

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
}
