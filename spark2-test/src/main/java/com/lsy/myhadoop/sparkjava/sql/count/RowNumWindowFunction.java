package com.lsy.myhadoop.sparkjava.sql.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by lisiyu on 2016/4/6.
 */
public class RowNumWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RowNumWindowFunction")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("DROP TABLE IF EXISTS sales");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
                + "product STRING,"
                + "category STRING,"
                + "revenue BIGINT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/root/Desktop/sales.txt' INTO TABLE sales");

        DataFrame top3SalesDF = hiveContext.sql(""
                +"SELECT product,category,revenue FROM ("
                + "SELECT product,category,revenue,"
                + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank FROM sales)"
                + " tmp_sales WHERE rank<=3");

        hiveContext.sql("DROP TABLE IF EXISTS top3_sales");
        top3SalesDF.show();
        top3SalesDF.saveAsTable("top3_sales");

        sc.close();
    }
}
