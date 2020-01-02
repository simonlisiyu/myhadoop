package com.lsy.myhadoop.sparkjava.sql.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by root on 4/5/16.
 */
public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().load("hdfs://192.168.190.132:9000/test/users.parquet");

        df.registerTempTable("users");
        DataFrame userNameDF = sqlContext.sql("select name from users");

        List<String> userNames = userNameDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: "+row.getString(0);
            }
        }).collect();

        for(String userName : userNames){
            System.out.println(userName);
        }

    }
}
