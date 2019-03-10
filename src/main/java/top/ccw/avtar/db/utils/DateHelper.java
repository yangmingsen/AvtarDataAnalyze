package top.ccw.avtar.db.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateHelper {

    private static final String PATTERN1 = "yyyy-MM-dd";
    private static final String PATTERN2 = "yyyy-MM-dd HH:mm";
    private static final String PATTERN3 = "yyyy-MM";
    private static final String PATTERN4 = "yyyy-MM-dd HH:mm:ss";
    private static final String PATTERN5 = "yyyy.MM.dd HH:mm:ss";
    private static final String PATTERN6 = "yyyy年MM月dd日HH时";


    private static String match(String pattern) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String timeStampStr = simpleDateFormat.format(date);
        return timeStampStr;
    }

    public static String getYYYY_MM_DD() {
        return match(PATTERN1);
    }

    public static String getYYYY_MM_DD_HH_MM() {
        return match(PATTERN2);
    }

    public static String getYYYY_MM() {
        return match(PATTERN3);
    }

    public static String getYYYY_MM_DD_HH_MM_SS() {
        return match(PATTERN4);
    }

    public static String getYYYY_MM_DD_HH_MM_SS_() {
        return match(PATTERN5);
    }

    public static String getYYYY_MM_DD_HH_MM_SS__() {
        return match(PATTERN6);
    }
}
