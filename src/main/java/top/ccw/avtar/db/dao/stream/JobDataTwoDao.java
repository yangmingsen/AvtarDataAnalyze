package top.ccw.avtar.db.dao.stream;

import top.ccw.avtar.entity.JobDataTwo;
import top.ccw.avtar.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class JobDataTwoDao {
    private static Connection conn = null;
    private static JobDataTwoDao jobDataTwoDaoSingle = null;

    private JobDataTwoDao() {
        try {
            conn = JdbcUtil.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 加synchronized同步锁或是用同步代码块对类加同步锁.
     * 使用双重检查进一步做了优化，可以
     * 避免整个方法被锁，只对需要锁的代码部分加锁，可以提高执行效率
     * @return
     */
    public static JobDataTwoDao getInstance() {
        if(jobDataTwoDaoSingle == null) {
            synchronized (JobDataTwoDao.class) {
                if(jobDataTwoDaoSingle == null) {
                    jobDataTwoDaoSingle = new JobDataTwoDao();
                }
            }
        }

        return jobDataTwoDaoSingle;
    }

    public boolean inserts(List<JobDataTwo> jobs) {
        PreparedStatement pstmt = null;

        String sql = "INSERT INTO tb_jobinfo_data (`direction`,`job_name`,`company_name`,`job_site_provinces`,"
                + "`job_site`,`job_salary_min`,`job_salary_max`,`relase_date`,`education_level`,"
                + "`work_exper`,`company_welfare`,`job_require`,`company_type`,`company_people_num`,`company_business`)"
                + "VALUES(?,?,?,?,?,"
                + "?,?,?,?,?,"
                + "?,?,?,?,?)";

        try {
            pstmt = conn.prepareStatement(sql);

            for (JobDataTwo job : jobs) {

                pstmt.setInt(1, job.getDirection());
                pstmt.setString(2, job.getJobName());
                pstmt.setString(3, job.getCompanyName());
                pstmt.setString(4, job.getJobSiteProvinces());
                pstmt.setString(5, job.getJobSite());
                pstmt.setString(6, job.getJobSalaryMin());
                pstmt.setString(7, job.getJobSalaryMax());
                pstmt.setString(8, job.getRelaseDate());
                pstmt.setString(9, job.getEducationLevel());
                pstmt.setString(10, job.getWorkExper());
                pstmt.setString(11, job.getCompanyWelfare());
                pstmt.setString(12, job.getJobRequire());
                pstmt.setString(13, job.getCompanyType());
                pstmt.setString(14, job.getCompanyPeopleNum());
                pstmt.setString(15, job.getCompanyBusiness());

                pstmt.addBatch();

            }

            pstmt.executeBatch();
            System.out.println("插入执行完毕...");

            return true;
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            JdbcUtil.release(null,pstmt,null);
        }

    }

    public boolean insert(JobDataTwo j2) {
        PreparedStatement pstmt = null;

        String sql = "INSERT INTO tb_jobinfo_data (`direction`,`job_name`,`company_name`,`job_site_provinces`,"
                + "`job_site`,`job_salary_min`,`job_salary_max`,`relase_date`,`education_level`,"
                + "`work_exper`,`company_welfare`,`job_require`,`company_type`,`company_people_num`,`company_business`)"
                + "VALUES(?,?,?,?,?,"
                + "?,?,?,?,?,"
                + "?,?,?,?,?)";

        try {
            pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1, j2.getDirection());
            pstmt.setString(2, j2.getJobName());
            pstmt.setString(3, j2.getCompanyName());
            pstmt.setString(4, j2.getJobSiteProvinces());
            pstmt.setString(5, j2.getJobSite());
            pstmt.setString(6, j2.getJobSalaryMin());
            pstmt.setString(7, j2.getJobSalaryMax());
            pstmt.setString(8, j2.getRelaseDate());
            pstmt.setString(9, j2.getEducationLevel());
            pstmt.setString(10, j2.getWorkExper());
            pstmt.setString(11, j2.getCompanyWelfare());
            pstmt.setString(12, j2.getJobRequire());
            pstmt.setString(13, j2.getCompanyType());
            pstmt.setString(14, j2.getCompanyPeopleNum());
            pstmt.setString(15, j2.getCompanyBusiness());

            return true;
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            JdbcUtil.release(null,pstmt,null);
        }


    }

}
