package top.ccw.avtar.instream.clearn;

import com.google.common.base.CharMatcher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

/***
 * 描述： 这是数据清洗程序
 *
 *修改时时间：
 * 修改者：
 *
 * 建立时间：2019/03/09
 * 建立者： yangmingsen
 */
public class DataClean {

    public static void cleandata() {

        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);

        CleanDataDao cleanDataDao = new CleanDataDao();
        Connection conn = DbUtil.getConn();

        PreparedStatement psmt = null;
        ResultSet rs = null;

        String sql = "SELECT job_name, company_name, job_site, job_salary,relase_date, job_info1,company_welfare, " +
                "job_info2,company_type, company_people_num, company_business FROM software_enginee LIMIT 22062,7455";

        try {
            psmt = conn.prepareStatement(sql);
            rs = psmt.executeQuery();
            while (rs.next()) {
                int direction = 10;
                String job_name = rs.getString("job_name");
                String company_name = rs.getString("company_name");
                String job_site = rs.getString("job_site");
                String job_salary = rs.getString("job_salary");
                //Double avr_salary = 0.0;
                String job_info = rs.getString("job_info1");
                String education_level = null;
                String work_exper = null;
                String job_resp = null;
                String job_require = null;

                if (job_salary.contains("千/月")) {
                    double firstnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[0]);
                    double secondnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[1]);
                    //avr_salary = (firstnum + secondnum) / 2;
                    //avr_salary = Double.valueOf(String.format("%.0f", avr_salary));
                    job_salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
                }
                if (job_salary.contains("万/月")) {
                    double firstnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[0])
                            * 10;
                    double secondnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[1])
                            * 10;
                    //avr_salary = (firstnum + secondnum) / 2;
                    //avr_salary = Double.valueOf(String.format("%.0f", avr_salary));
                    job_salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
                }
                if (job_salary.contains("万/年")) {
                    double firstnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[0])
                            / 1.2;
                    double secondnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3).split("-")[1])
                            / 1.2;
                    //avr_salary = (firstnum + secondnum) / 2;
                    //avr_salary = Double.valueOf(String.format("%.0f", avr_salary));
                    job_salary = String.format("%.0f", firstnum) + "-" + String.format("%.0f", secondnum) + "千/月";
                }
                if (job_salary.contains("元/天")) {
                    double firstnum = Double.valueOf(job_salary.substring(0, job_salary.length() - 3)) * 0.03;
                    //avr_salary = Double.valueOf(String.format("%.0f", firstnum));
                    job_salary = String.format("%.0f", firstnum) + "千/月";
                }
                String relase_date = year + "-" + rs.getString("relase_date");

                if (job_info != "") {
                    education_level = CharMatcher.WHITESPACE.trimFrom(job_info).split("\\|")[2];
                    work_exper = CharMatcher.WHITESPACE.trimFrom(job_info).split("\\|")[1];
                    if (!education_level.contains("本科") && !education_level.contains("大专") && !education_level
                            .contains("博士") && !education_level.contains("硕士") && !education_level.contains("中专") &&
                            !education_level.contains("高中")) {
                        if (job_info.contains("本科"))
                            education_level = "本科";
                        else if (job_info.contains("大专"))
                            education_level = "大专";
                        else if (job_info.contains("博士"))
                            education_level = "博士";
                        else if (job_info.contains("硕士"))
                            education_level = "硕士";
                        else if (job_info.contains("中专"))
                            education_level = "中专";
                        else if (job_info.contains("高中"))
                            education_level = "高中";
                        else
                            education_level = "";
                    }
                }
                String company_welfare = rs.getString("company_welfare").replaceAll("\\t", "");
                if (rs.getString("job_info2") != "") {
                    job_resp = rs.getString("job_info2").replaceAll("\\t", "").replaceAll("[ ]+", " ").replaceAll
                            ("(?m)^\\s*$(\\n|\\r\\n)", "");
                    int x = job_resp.indexOf("职能类别");
                    job_require = rs.getString("job_info2").replaceAll("\\t", "").replaceAll("[ ]+", " ")
                            .replaceAll("(?m)^\\s*$(\\n|\\r\\n)", "").substring(4, x);
                }

                String company_type = rs.getString("company_type");
                String company_people_num = rs.getString("company_people_num");
                String company_business = rs.getString("company_business");

                cleanDataDao.insert(direction, job_name, company_name, job_site, job_salary, relase_date,
                        education_level, work_exper, company_welfare, job_require, job_require, company_type,
                        company_people_num, company_business);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DbUtil.close(rs, psmt, conn);
        }
    }

    /*public static void main(String[] args) {
        cleandata();
    }*/

}
