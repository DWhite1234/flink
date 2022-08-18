package utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;



public class JDBCutils {

    public static Logger logger = LoggerFactory.getLogger(JDBCutils.class);
    public static Properties props = new Properties();
    public static DataSource ds;

    /**
     * 初始化druid连接池
     */
    static {
        try {
            InputStream inputStream = JDBCutils.class.getClassLoader().getResourceAsStream("test/druid.properties");
            props.load(inputStream);
            ds = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            logger.error("druid连接池对象获取失败....",e);
        }
    }


    /**
     * 释放资源
     * @param ps
     * @param con
     * @param rs
     */
    public static void close(PreparedStatement ps, Connection con, ResultSet rs) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static ArrayList<String> select(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        ArrayList<String> list = new ArrayList<>();
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            while (rs.next()){
                StringBuffer buffer = new StringBuffer();
                for (int i = 1; i <=count; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object object = rs.getObject(columnName);
                    buffer.append(columnName).append("_").append(object).append(":");
                }

                list.add(buffer.toString());
            }

        } catch (SQLException e) {
            logger.error("sql查询失败......",e);
        } finally {
            JDBCutils.close(ps,conn,rs);
        }

        return list;

    }


    public static void main(String[] args) {
        ArrayList<String> list = JDBCutils.select("select * from product");
        System.out.println(list);
    }
}
