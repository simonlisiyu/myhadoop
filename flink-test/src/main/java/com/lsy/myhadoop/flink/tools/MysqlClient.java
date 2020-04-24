package com.lsy.myhadoop.flink.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.lsy.myhadoop.flink.domain.bus_pojo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlClient {
    private static DruidPooledConnection conn;
    private static PreparedStatement ps;

    static {
        try {
            GetDPMysqlconnetion ds = new GetDPMysqlconnetion();
            conn = ds.getconnectionpool();
            ps = conn.prepareStatement(" SELECT direction, real_time_start FROM TBL_EHUALU_TRIP " +
                    "WHERE line_name = ?" +
                    " AND bus_self_id = ? AND real_time_start IS NOT NULL AND UNIX_TIMESTAMP(?) - UNIX_TIMESTAMP" +
                    "(real_time_start) = ( SELECT MIN(UNIX_TIMESTAMP(?)-UNIX_TIMESTAMP(real_time_start)) FROM " +
                    "TBL_EHUALU_TRIP WHERE line_name = ? AND bus_self_id = ? AND real_time_start IS NOT " +
                    "NULL)");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }





    public bus_pojo query1(bus_pojo bus_pojo) {

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            String line_no = bus_pojo.getLine_no();
            String bus_no = bus_pojo.getBus_no();
            String mtime = bus_pojo.getMtime();
            ps.setString(1, line_no);
            ps.setString(2, bus_no);
            ps.setObject(3, mtime);
            ps.setObject(4, mtime);
            ps.setString(5, line_no);
            ps.setString(6, bus_no);
            ResultSet rs = ps.executeQuery();
            if (!rs.isClosed() && rs.next()) {
                bus_pojo.setStime((String) rs.getObject("real_time_start"));
                String direction = rs.getString("direction");
                if (direction.equals("1")){bus_pojo.setUp_or_down("上行");}
                else bus_pojo.setUp_or_down("下行");
            }
            else {
                bus_pojo.setStime("未找到发车时间");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return bus_pojo;

    }
}
