package com.lsy.myhadoop.flink.tools;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.SQLException;

public class GetDPMysqlconnetion {
    public  DruidDataSource GetDataPool(){
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUrl("jdbc:mysql://10.128.3.3:3306/sbg?useSSL=false&autoReconnect=true" +
                "&failOverReadOnly=false&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&serverTimezone=Asia/Shanghai");
        ds.setUsername("root");
        ds.setPassword("sb_pass");
//      配置初始化大小、最小、最大
        ds.setInitialSize(1);
        ds.setMinIdle(1);
        ds.setMaxActive(20);
//      获取链接等待超时时间
        ds.setMaxWait(20000);
//      检测需要关闭的连接的间隔时间
        ds.setTimeBetweenEvictionRunsMillis(20000);
//      防止连接过期
        ds.setValidationQuery("SELECT 'x'");
        ds.setTestWhileIdle(true);
        ds.setTestOnBorrow(true);
        return ds;
    }
    public DruidPooledConnection getconnectionpool() throws SQLException {
        DruidDataSource ds = GetDataPool();
        DruidPooledConnection connection = ds.getConnection();
        return connection;
    }
}
