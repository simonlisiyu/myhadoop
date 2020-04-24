package com.lsy.myhadoop.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html
 * Created by lisiyu on 2020/4/23.
 */
public class HdfsFileTest {


    public static void main(String[] args) {


        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://jiaotong.danao.test1:8020");
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs= null;
        try {
            fs = FileSystem.get(new URI("hdfs://jiaotong.danao.test1:8020"),conf,"hdfs");
//            fs.deleteOnExit(new Path("/user/shenba/test/t1"));
            System.out.println(fs.getFileStatus(new Path("/user/shenba")));

            FileStatus[] statuses = fs.listStatus(new Path("/user/shenba"));
            for (FileStatus status : statuses){
                System.out.println(status);
            }

            ContentSummary cs = fs.getContentSummary(new Path("/user/shenba"));
            long fileCount = cs.getFileCount();
            System.out.println(cs);
            System.out.println(fileCount);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }



//        try {
//            UserGroupInformation ugi
//                    = UserGroupInformation.createRemoteUser("hbase");
//
//            ugi.doAs(new PrivilegedExceptionAction<Void>() {
//
//                public Void run() throws Exception {
//
//                    Configuration conf = new Configuration();
//                    conf.set("fs.defaultFS", "hdfs://jiaotong.danao.test1:9000/user/hbase");
//                    conf.set("hadoop.job.ugi", "hbase");
//
//                    FileSystem fs = FileSystem.get(conf);
//
//                    fs.createNewFile(new Path("/user/hbase/test"));
//
//                    FileStatus[] status = fs.listStatus(new Path("/user/hbase"));
//                    for(int i=0;i<status.length;i++){
//                        System.out.println(status[i].getPath());
//                    }
//                    return null;
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }


}
