package top.ccw.avtar.db.dao;

import top.ccw.avtar.db.entity.TimeSalaryOne;
import top.ccw.avtar.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TimeSalaryOneDao {

    private static Connection conn = null;
    private static TimeSalaryOneDao timeSalaryOneDaoSingle = null;

    private TimeSalaryOneDao() {
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
    public static TimeSalaryOneDao getInstance() {
        if(timeSalaryOneDaoSingle == null) {
            synchronized (TimeSalaryOneDao.class) {
                if(timeSalaryOneDaoSingle == null) {
                    timeSalaryOneDaoSingle = new TimeSalaryOneDao();
                }
            }
        }

        return timeSalaryOneDaoSingle;
    }

    public boolean insert(TimeSalaryOne tso) {
        PreparedStatement pstmt = null;

        String sql = "INSERT INTO `job_data`.`tb_current_time_salary`(`jobtype_two_id`, `time`, `result`, `forecast`) VALUES (?, ?, ?, ?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,tso.getJobtypeTwoId());
            pstmt.setString(2,tso.getTime());
            pstmt.setString(3,tso.getResult());
            pstmt.setString(4,tso.getForecast());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            return  false;
        }finally {
            JdbcUtil.release(null,pstmt,null);
        }

        return true;

    }

    public boolean update(TimeSalaryOne tso) {
        PreparedStatement pstmt = null;

        String sql = "UPDATE `job_data`.`tb_current_time_salary` SET `jobtype_two_id` = ?, `time` = ?, `result` = ?, `forecast` = ? WHERE `id` = ?";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,tso.getJobtypeTwoId());
            pstmt.setString(2,tso.getTime());
            pstmt.setString(3,tso.getResult());
            pstmt.setString(4,tso.getForecast());
            pstmt.setInt(5,tso.getId());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            return  false;
        }finally {
            JdbcUtil.release(null,pstmt,null);
        }


        return true;
    }



    public Integer searchId(Integer jobtypeTwoId, String time) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT id FROM `tb_current_time_salary` WHERE jobtype_two_id=? AND time=?";

        try {
            pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1,jobtypeTwoId);
            pstmt.setString(2,time);

            rs = pstmt.executeQuery();

            while (rs.next()) {
                return rs.getInt(1);
            }


        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(null,pstmt,rs);
        }

        return 0;
    }


}
