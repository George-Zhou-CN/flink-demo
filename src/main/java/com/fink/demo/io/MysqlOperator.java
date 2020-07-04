package com.fink.demo.io;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 8:08 下午
 * @Desc 类目维度表
 **/
public class MysqlOperator {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/flink";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "123456";

    public List<Map<String, Object>> query(String sql) throws Exception {
        Connection conn = null;
        Statement sm = null;
        ResultSet rs = null;

        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            conn = getConn();
            sm = conn.createStatement();
            rs = sm.executeQuery(sql);

            while (rs.next()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                for (int i = 1; i <= colCount; i++) {
                    Map<String, Object> row = new HashMap<>();
                    String colName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(colName, value);
                    rows.add(row);
                }
            }

            return rows;
        } catch (Exception e) {
            e.getStackTrace();
            throw e;
        } finally {
            closeConn(conn, sm, rs);
        }
    }

    private Connection getConn() throws Exception {
        Class.forName(JDBC_DRIVER);
        return DriverManager.getConnection(DB_URL, USER_NAME, PASSWORD);
    }

    private void closeConn(Connection conn, Statement sm, ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (sm != null) {
            sm.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
