package com.lsy.myhadoop.cloudera;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.v14.RootResourceV14;
import com.lsy.myhadoop.cloudera.cm.config.CmConfig;
import com.lsy.myhadoop.cloudera.cm.hdfs.HadoopClient;

import java.util.List;

/**
 * Created by lisiyu on 2020/4/23.
 */
public class HdfsFileUtilsTest {


    public static void main(String[] args) {
        CmConfig config = new CmConfig();
        config.setAddress("jiaotong.danao.test1");
        config.setPort(7180);
        config.setUsername("admin");
        config.setPassword("admin");

        HadoopClient client = new HadoopClient();
        RootResourceV14 rootApi = client.rootResource(config);

        String defaultClusterName = "";
        ApiClusterList clusters = rootApi.getClustersResource().readClusters(DataView.FULL);
        List<ApiCluster> clustersInfo = clusters.getClusters();
        if(clustersInfo!=null&&clustersInfo.size()>0) {
            //默认取第一个cluster的name
            defaultClusterName = clustersInfo.get(0).getName();
        }else {
            throw new RuntimeException("no cluster found");
        }

        System.out.println(defaultClusterName);
    }


}
