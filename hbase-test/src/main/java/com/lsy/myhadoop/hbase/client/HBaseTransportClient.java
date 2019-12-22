package com.lsy.myhadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 获取Hbase客户端
 *
 * */

public class HBaseTransportClient {
    private static Logger LOG = LoggerFactory.getLogger(HBaseTransportClient.class);

    private static Configuration conf = HBaseConfiguration.create();
    private static HConnection conn = null;
    private static HBaseAdmin admin = null;

    public synchronized static HConnection getConn(String zkQuorum) {
        return getConn(zkQuorum, "", "",
                0, 0, 0, "");
    }
    /**
     * 初始化HBase client Connection
     * @param zkQuorum
     * @param zkPath default=""("/hbase")
     * @param tmpDir    default=""
     * @param threadMax default=0(256)，线程池维护线程的最大数量
     * @param threadMin default=0(256)，线程池维护线程的最少数量
     * @param threadTTL default=0(60)，线程池维护线程所允许的空闲时间
     * @param hbaseUserName default=""
     * @return
     */
    public synchronized static HConnection getConn(String zkQuorum, String zkPath, String tmpDir,
                                                   int threadMax, int threadMin, int threadTTL,
                                                   String hbaseUserName) {
        LOG.info("enter HBaseClient.getConn(), zkQuorum="+zkQuorum+", zkPath="+zkPath+", tmpDir="+tmpDir
                +", threadMax="+threadMax+", threadMin="+threadMin+", threadTTL="+threadTTL);
        if (conn == null) {
            LOG.debug("hbase.zookeeper.quorum="+zkQuorum);
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            if(!"".equals(hbaseUserName)) conf.set("hbase.user.name", hbaseUserName);
            if(!"".equals(zkPath)) conf.set("zookeeper.znode.parent", zkPath);
            if(!"".equals(tmpDir)) conf.set("hbase.fs.tmp.dir", tmpDir);
            if(0 != threadMax) conf.set("hbase.hconnection.threads.max", threadMax+"");
            if(0 != threadMin) conf.set("hbase.hconnection.threads.core", threadMin+"");
            if(0 != threadTTL) conf.set("hbase.hconnection.threads.keepalivetime", threadTTL+"");
            LOG.debug("zookeeper.znode.paren="+zkPath);

            try {
                if (conf.get("hbase.user.name") != null) {
                    UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser(conf.get("hbase.user.name"));
                    conn = HConnectionManager.createConnection(conf, User.create(userGroupInformation));
                } else
                    conn = HConnectionManager.createConnection(conf);
                LOG.debug("enter HBaseClient.getConn()， conn is null="+(conn==null));
            } catch (IOException e) {
                LOG.error("Fail to getConn(). Exception=" + ExceptionUtils.getStackTrace(e));
            }

        }
        LOG.debug("success HBaseClient.getConn()");
        return conn;
    }

    public synchronized static HBaseAdmin getAdmin(String zkQuorum) {
        LOG.debug("enter HBaseClient.getAdmin()");
        if(admin == null){
            try {
                conf.set("hbase.zookeeper.quorum", zkQuorum);
                admin = new HBaseAdmin(conf);
            } catch (IOException e) {
                LOG.error("Fail to getAdmin(). Exception=" + ExceptionUtils.getStackTrace(e));
            }
        }
        LOG.debug("success HBaseClient.getAdmin()");
        return admin;
    }

    public synchronized static void closetConn() {
        LOG.debug("enter HBaseClient.closetConn()");
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                LOG.error("Fail to closetConn(). Exception=" + ExceptionUtils.getStackTrace(e));
            } finally {
                conn = null;
            }
        }
        LOG.debug("success HBaseClient.closetConn()");
    }

    public synchronized static void closetAdmin() {
        LOG.debug("enter HBaseClient.closetAdmin()");
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                LOG.error("Fail to closetAdmin(). Exception=" + ExceptionUtils.getStackTrace(e));
            } finally {
                admin = null;
            }
        }
        LOG.debug("success HBaseClient.closetAdmin()");
    }



}
