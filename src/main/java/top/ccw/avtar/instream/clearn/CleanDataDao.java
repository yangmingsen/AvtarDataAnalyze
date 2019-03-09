package top.ccw.avtar.instream.clearn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @program: AvtarDataAnalyze
 * @description:
 * @author: ljq
 * @create: 2019-03-09 20:48
 **/

public class CleanDataDao {
    public void insert(int direction, String job_name, String company_name, String job_site, String
            job_salary, String relase_date, String education_level, String work_exper, String
                               company_welfare, String job_resp, String job_require, String
                               company_type, String company_people_num, String company_business) {
        Connection conn = DbUtil.getConn();
        PreparedStatement psmt = null;
        String sql = "INSERT INTO tb_job_info(direction, job_name, company_name, job_site, job_salary," +
                "relase_date, " +
                "education_level, work_exper, company_welfare, job_resp, job_require,company_type,company_people_num," +
                " company_business) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            psmt = conn.prepareStatement(sql);
            psmt.setInt(1, direction);
            psmt.setString(2, job_name);
            psmt.setString(3, company_name);
            psmt.setString(4, job_site);
            psmt.setString(5, job_salary);
            psmt.setString(6, relase_date);
            psmt.setString(7, education_level);
            psmt.setString(8, work_exper);
            psmt.setString(9, company_welfare);
            psmt.setString(10, job_resp);
            psmt.setString(11, job_require);
            psmt.setString(12, company_type);
            psmt.setString(13, company_people_num);
            psmt.setString(14, company_business);
            psmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DbUtil.close(psmt, conn);
        }
    }

}
