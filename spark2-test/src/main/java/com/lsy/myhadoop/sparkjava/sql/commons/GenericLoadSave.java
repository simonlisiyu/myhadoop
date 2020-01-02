package com.lsy.myhadoop.sparkjava.sql.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 4/5/16.
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().load("hdfs://192.168.190.132:9000/test/users.parquet");
        df.printSchema();
        df.show();

        df.select("name","favorite_color").write().save("hdfs://192.168.190.132:9000/testoutput/");
    }
}
