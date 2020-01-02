package com.lsy.myhadoop.sparkjava.sql.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 4/6/16.
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JSONDataSource")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://192.168.190.132:9000/test/students080.json");

        df.registerTempTable("studentTable");

        DataFrame goodStudentScoreDF = sqlContext.sql("select name,score from studentTable where score >= 80");

        List<String> goodStudentNames = goodStudentScoreDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> studentInfoJSONs = new ArrayList<String>();
        studentInfoJSONs.add("{\"name\":\"Leo\",\"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\",\"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\",\"age\":19}");

        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);
        DataFrame studentInfoDF = sqlContext.read().json(studentInfoJSONsRDD);

        studentInfoDF.registerTempTable("studentInfo");

        String sql = "select name,age from studentInfo where name in ( ";
        for(int i=0;i<goodStudentNames.size();i++){
            sql +="'"+goodStudentNames.get(i)+"'";
            if(i<goodStudentNames.size()-1){
                sql += ",";
            }
        }
        sql += ")";

        DataFrame goodStudentInfoDF = sqlContext.sql(sql);

        JavaPairRDD<String, Tuple2<Integer,Integer>> goodStudentsRDD =
                goodStudentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));

        JavaRDD<Row> goodStudentsRowRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2());
            }
        });

        List<StructField> structField = new ArrayList<>();
        structField.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structField.add(DataTypes.createStructField("score", DataTypes.IntegerType,true));
        structField.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
        StructType stuctType = DataTypes.createStructType(structField);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentsRowRDD, stuctType);

        goodStudentsDF.write().format("json").save("hdfs://192.168.190.132:9000/test/080");

    }
}
