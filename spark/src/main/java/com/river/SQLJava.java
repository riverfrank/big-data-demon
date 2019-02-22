package com.river;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author river
 * @date 2019/2/22 9:38
 **/
public class SQLJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("sql")
                .config("spark.master","local")
                .getOrCreate();
        Dataset<Row> df1 = spark.read().json("file:///E:/myTest/spark-demo/json.txt");
        //创建临时视图
        df1.createOrReplaceTempView("customers");
        df1.show();

        df1.select("id").where("id > 1").show();

        //按照sql方式查询
        Dataset<Row> df2 = spark.sql("select * from customers where age > 25 ");
        df2.show();
        System.out.println("=================");
    }
}
