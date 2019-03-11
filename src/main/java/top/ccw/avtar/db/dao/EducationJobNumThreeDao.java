package top.ccw.avtar.db.dao;

import top.ccw.avtar.db.entity.EducationJobNumThree;
import top.ccw.avtar.db.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EducationJobNumThreeDao {
    private static Connection conn = null;
    private static EducationJobNumThreeDao educationJobNumThreeDaoSingle = null;

    private EducationJobNumThreeDao() {
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
    public static EducationJobNumThreeDao getInstance() {
        if(educationJobNumThreeDaoSingle == null) {
            synchronized (EducationJobNumThreeDao.class) {
                if(educationJobNumThreeDaoSingle == null) {
                    educationJobNumThreeDaoSingle = new EducationJobNumThreeDao();
                }
            }
        }

        return educationJobNumThreeDaoSingle;
    }

    public boolean insert(EducationJobNumThree ejt) {
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO `job_data`.`tb_current_education_jobnum`(`jobtype_two_id`, `result`,`time`) VALUES (?,?,?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,ejt.getJobtypeTwoId());
            pstmt.setString(2,ejt.getResult());
            pstmt.setString(3,ejt.getDate());

            pstmt.executeUpdate();

            return true;

        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean update(EducationJobNumThree ejt) {
        PreparedStatement pstmt = null;
        String sql = "UPDATE `job_data`.`tb_current_education_jobnum` SET `jobtype_two_id` = ?, `result` = ?,`time` = ? WHERE `id` = ?";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,ejt.getJobtypeTwoId());
            pstmt.setString(2,ejt.getResult());
            pstmt.setString(3,ejt.getDate());
            pstmt.setInt(4,ejt.getId());

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
        String sql = "SELECT id FROM `tb_current_education_jobnum` WHERE jobtype_two_id=? AND time=?";

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
