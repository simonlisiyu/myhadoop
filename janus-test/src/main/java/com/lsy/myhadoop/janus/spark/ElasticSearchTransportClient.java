package com.lsy.myhadoop.janus.spark;


import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class ElasticSearchTransportClient {
        public List<PoliceEntity> run1(){
            SparkConf conf = new SparkConf();
            conf.setMaster("local[1]");
            conf.setAppName("SPARK ES");

            conf.set("es.index.auto.create", "true");
            //conf.set("es.nodes","127.0.0.1");
            conf.set("es.nodes", "10.179.40.207");
            conf.set("es.port", "9200");

            JavaSparkContext sc = new JavaSparkContext(conf);
            sc.setLogLevel("ERROR");

            JavaPairRDD<String, Map<String, Object>> esRDD =
                    JavaEsSpark.esRDD(sc, "police_1_101_1_test/police_1_101_1");

            List<PoliceEntity> policeData = Lists.newArrayList();

            for(Tuple2 tuple:esRDD.collect()){
                String every = tuple._2().toString();
                PoliceEntity singlePolice = new PoliceEntity(every.split(",")[2].split("=")[1],every.split(",")[3].split("=")[1]);
                policeData.add(singlePolice);
        }
        return policeData;
    }
    public static void main(String[] args) {
        ElasticSearchTransportClient client = new ElasticSearchTransportClient();
        List<PoliceEntity> policeData = client.run1();
        JanusEntityWriter policeWrite = new JanusEntityWriter();
        policeWrite.dataWrite(policeData);
        System.out.println("Data write success!");

    }
}
