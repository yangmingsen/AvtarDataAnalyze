package top.ccw.avtar.db.dao;



import top.ccw.avtar.db.entity.SalarySiteTwo;
import top.ccw.avtar.db.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SalarySiteTwoDao {
    private static Connection conn = null;
    private static SalarySiteTwoDao salarySiteTwoDaoSingle = null;

    private SalarySiteTwoDao() {
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
    public static SalarySiteTwoDao getInstance() {
        if(salarySiteTwoDaoSingle == null) {
            synchronized (SalarySiteTwoDao.class) {
                if(salarySiteTwoDaoSingle == null) {
                    salarySiteTwoDaoSingle = new SalarySiteTwoDao();
                }
            }
        }

        return salarySiteTwoDaoSingle;
    }

    public boolean insert(SalarySiteTwo sst) {
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO `job_data`.`tb_current_salary_site`(`jobtype_two_id`, `result`) VALUES (?,?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,sst.getJobtypeTwoId());
            pstmt.setString(2,sst.getResult());

            pstmt.executeUpdate();

            return true;

        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean update(SalarySiteTwo sst) {
        PreparedStatement pstmt = null;
        String sql = "UPDATE `job_data`.`tb_current_salary_site` SET `jobtype_two_id` = ?, " +
                "`result` = ?,`time` = ?, `require_max` = ?, `salary_max` = ? WHERE `id` = ?";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,sst.getJobtypeTwoId());
            pstmt.setString(2,sst.getResult());
            pstmt.setString(3,sst.getDate());
            pstmt.setString(4,sst.getRequireMax());
            pstmt.setString(5,sst.getSalaryMax());
            pstmt.setInt(6,sst.getId());

            pstmt.executeUpdate();

            return true;

        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Integer searchId(Integer jobtypeTwoId, String time) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT id FROM `tb_current_salary_site` WHERE jobtype_two_id=? AND time=?";

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
        }

        return 0;
    }

}
