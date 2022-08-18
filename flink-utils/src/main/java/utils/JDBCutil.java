package utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;


public class JDBCutil{

    public static Logger logger = LoggerFactory.getLogger(JDBCutil.class);
    public static Properties props = new Properties();
    public static DataSource ds;

    /**
     * 初始化druid连接池
     */
    static {
        try {
            InputStream inputStream = JDBCutil.class.getClassLoader().getResourceAsStream("druid.properties");
            Properties props = new Properties();
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

    public static void close(PreparedStatement ps, Connection con) {
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
    }


    //查询期初金额和矫正日期
    public static Tuple2<BigDecimal,String> selectInitMoney(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Tuple2<BigDecimal,String> initAndCheck = new Tuple2<>();
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            if (count>0) {
                if (rs.next()) {
                    initAndCheck.f0 = rs.getBigDecimal(metaData.getColumnName(1));
                    String dateTime = rs.getString(metaData.getColumnName(2));
                    if (dateTime!="") {
                        initAndCheck.f1 = dateTime;
                    }else{
                        initAndCheck.f1 = null;
                    }

                }

            }else{
                initAndCheck.f0 = new BigDecimal(0);
                initAndCheck.f1 = null;
            }

            JDBCutil.close(ps,conn,rs);

        } catch (SQLException e) {
            logger.error("sql查询失败......",e);
        }

        return initAndCheck;

    }

    //查询同业账户
    public static String selectInterBankAccount(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String interBankAccount = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            if (rs.next()) {
                interBankAccount = rs.getString(metaData.getColumnName(1));
            }

            JDBCutil.close(ps,conn,rs);

        } catch (SQLException e) {
            logger.error("sql查询失败......",e);
        }

        return interBankAccount;

    }


    //查询uuid和期初余额
    public static Tuple2<String,String> selectCheckInfo(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Tuple2<String,String> tuple2 = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            if (rs.next()) {
                tuple2.f0 = rs.getString(metaData.getColumnName(1));
                tuple2.f1 = rs.getString(metaData.getColumnName(2));
            }

            JDBCutil.close(ps,conn,rs);

        } catch (SQLException e) {
            logger.error("sql查询失败......",e);
        }

        return tuple2;

    }


    //矫正余额
    public static void updateBalance(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        Integer rs = null;
        String interBankAccount = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeUpdate();

            JDBCutil.close(ps,conn);

        } catch (SQLException e) {
            logger.error("sql查询失败......",e);
        }
    }

}
