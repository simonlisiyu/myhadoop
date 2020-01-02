package com.lsy.myhadoop.sparkjava.sql.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 4/5/16.
 */
public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameProgrammatically")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("hdfs://192.168.190.132:9000/test/students");
        JavaRDD<Row> rows = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] lineSplited = s.split(",");
                return RowFactory.create(Integer.parseInt(lineSplited[0].trim()),lineSplited[1], Integer.parseInt(lineSplited[2].trim()));
            }
        });

        List<StructField> structFieldsields = new ArrayList<StructField>();
        structFieldsields.add(DataTypes.createStructField("id", DataTypes.IntegerType,true));
        structFieldsields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structFieldsields.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFieldsields);

        DataFrame studentsDF = sqlContext.createDataFrame(rows, structType);

        studentsDF.registerTempTable("students");

        DataFrame youngerDF = sqlContext.sql("select * from students where age <= 18");

        List<Row> youngerList = youngerDF.javaRDD().collect();

        for(Row row : youngerList){
            System.out.println(row);
        }
    }
}
