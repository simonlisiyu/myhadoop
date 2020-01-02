package com.lsy.myhadoop.sparkjava.sql.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by root on 4/5/16.
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameReflection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("hdfs://192.168.190.132:9000/test/students");
        JavaRDD<Student> studentJavaRdd = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] lineSplited = s.split(",");
                Student stu = new Student();
                stu.setId(Integer.parseInt(lineSplited[0].trim()));
                stu.setName(lineSplited[1]);
                stu.setAge(Integer.parseInt(lineSplited[2].trim()));
                return stu;
            }
        });

        // reflection RDD to DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentJavaRdd, Student.class);

        // register DataFrame to a temple table, table could use by SQL.
        studentDF.registerTempTable("students");

        // search student age <= 18.
        DataFrame younger = sqlContext.sql("select * from students where age<=18");

        // put result to RDD
        JavaRDD<Row> youngerRDD = younger.javaRDD();

        // do map to youngerRDD<Student>
        JavaRDD<Student> studentRDD = youngerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                int id = row.getInt(1);
                String name = row.getString(2);
                int age = row.getInt(0);
                stu.setId(id);
                stu.setName(name);
                stu.setAge(age);
                return stu;
            }
        });

        // collect data back to driver, println
        List<Student> studentList = studentRDD.collect();
        for(Student stu : studentList){
            System.out.println(stu.toString());
        }


    }


}
