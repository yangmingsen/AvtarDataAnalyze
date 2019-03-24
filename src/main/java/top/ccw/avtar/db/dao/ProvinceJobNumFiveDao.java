package top.ccw.avtar.db.dao;

import top.ccw.avtar.db.entity.ProvinceJobNumFive;
import top.ccw.avtar.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProvinceJobNumFiveDao {
    private static Connection conn = null;
    private static ProvinceJobNumFiveDao provinceJobNumFiveDaoSingle = null;

    private ProvinceJobNumFiveDao() {
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
    public static ProvinceJobNumFiveDao getInstance() {
        if(provinceJobNumFiveDaoSingle == null) {
            synchronized (ProvinceJobNumFiveDao.class) {
                if(provinceJobNumFiveDaoSingle == null) {
                    provinceJobNumFiveDaoSingle = new ProvinceJobNumFiveDao();
                }
            }
        }

        return provinceJobNumFiveDaoSingle;
    }

    public boolean insert(ProvinceJobNumFive pjnf) {
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO `job_data`." +
                "`tb_current_province_jobnum`(`jobtype_two_id`, `result`,`time`,`day_num`,`week_num`,`result_two`) VALUES (?,?,?,?,?,?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,pjnf.getJobtypeTwoId());
            pstmt.setString(2,pjnf.getResult());
            pstmt.setString(3,pjnf.getDate());
            pstmt.setLong(4,pjnf.getDayNum());
            pstmt.setLong(5,pjnf.getWeekNum());
            pstmt.setString(6,pjnf.getResult_two());

            pstmt.executeUpdate();

            return true;

        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }finally {
            JdbcUtil.release(null,pstmt,null);
        }

    }

    public boolean update(ProvinceJobNumFive pjnf) {
        PreparedStatement pstmt = null;
        String sql = "UPDATE `job_data`.`tb_current_province_jobnum` SET " +
                "`jobtype_two_id` = ?, `result` = ?,`time` = ?,`day_num` = ?,`week_num` = ?,`result_two` = ? WHERE `id` = ?";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,pjnf.getJobtypeTwoId());
            pstmt.setString(2,pjnf.getResult());
            pstmt.setString(3,pjnf.getDate());
            pstmt.setLong(4,pjnf.getDayNum());
            pstmt.setLong(5,pjnf.getWeekNum());
            pstmt.setString(6,pjnf.getResult_two());
            pstmt.setInt(7,pjnf.getId());

            pstmt.executeUpdate();

            return true;

        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }finally {
            JdbcUtil.release(null,pstmt,null);
        }
    }

    public Integer searchId(Integer jobtypeTwoId, String time) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT id FROM `tb_current_province_jobnum` WHERE jobtype_two_id=? AND time=?";

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
