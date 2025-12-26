package nowcoder.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL 工具类
 * 用于将 MapReduce 结果写入 MySQL
 */
public class MySQLUtil {

    private static final String URL = "jdbc:mysql://10.128.30.233:3306/nowcoder_analysis?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8";
    private static final String USER = "root";
    private static final String PASSWORD = "111";

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL Driver not found", e);
        }
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    /**
     * 插入城市岗位统计结果
     */
    public static void insertCityJobCount(String city, int count) throws SQLException {
        String sql = "INSERT INTO city_job_count (city, job_count) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE job_count = ?, update_time = CURRENT_TIMESTAMP";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, city);
            pstmt.setInt(2, count);
            pstmt.setInt(3, count);
            pstmt.executeUpdate();
        }
    }

    /**
     * 批量插入城市岗位统计
     */
    public static void batchInsertCityJobCount(java.util.Map<String, Integer> cityCountMap) throws SQLException {
        String sql = "INSERT INTO city_job_count (city, job_count) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE job_count = ?, update_time = CURRENT_TIMESTAMP";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (java.util.Map.Entry<String, Integer> entry : cityCountMap.entrySet()) {
                pstmt.setString(1, entry.getKey());
                pstmt.setInt(2, entry.getValue());
                pstmt.setInt(3, entry.getValue());
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
        }
    }

    /**
     * 清空城市岗位统计表
     */
    public static void truncateCityJobCount() throws SQLException {
        String sql = "TRUNCATE TABLE city_job_count";
        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }
    }

    /**
     * 测试连接
     */
    public static boolean testConnection() {
        try (Connection conn = getConnection()) {
            return conn != null && !conn.isClosed();
        } catch (SQLException e) {
            System.err.println("数据库连接测试失败: " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
        // 测试连接
        if (testConnection()) {
            System.out.println("✓ MySQL 连接成功!");
        } else {
            System.out.println("✗ MySQL 连接失败!");
        }
    }
}
