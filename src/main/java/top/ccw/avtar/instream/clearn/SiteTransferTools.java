package top.ccw.avtar.instream.clearn;

import top.ccw.avtar.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SiteTransferTools {

    private static Connection conn = null;
    private static SiteTransferTools siteTransferToolsSingle = null;

    private SiteTransferTools() {
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
    public static SiteTransferTools getInstance() {
        if(siteTransferToolsSingle == null) {
            synchronized (SiteTransferTools.class) {
                if(siteTransferToolsSingle == null) {
                    siteTransferToolsSingle = new SiteTransferTools();
                }
            }
        }

        return siteTransferToolsSingle;
    }

    public String selectProvinceFromCity(String city) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String sql = "SELECT province FROM `tb_provinces` WHERE"
                + " provinceid=(SELECT provinceid FROM tb_cities WHERE city LIKE ?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "%"+city+"%");

            rs = pstmt.executeQuery();

            while(rs.next()) {
                return rs.getString("province");
            }

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(null, pstmt, rs);
        }

        return null;

    }


    public String selectCityFromAreas(String city) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String sql = "SELECT city FROM tb_cities WHERE "
                + "cityid = (SELECT cityid FROM tb_areas WHERE area LIKE ?)";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "%"+city+"%");

            rs = pstmt.executeQuery();

            while(rs.next()) {
                return rs.getString("city");
            }

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.release(null, pstmt, rs);
        }

        return null;

    }

    public String selectProvinceFromProvince(String city) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String sql = "SELECT province FROM `tb_provinces` WHERE province LIKE ?";

        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "%"+city+"%");

            rs = pstmt.executeQuery();

            while(rs.next()) {
                return rs.getString("province");
            }

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.release(null, pstmt, rs);
        }


        return null;
    }


}
