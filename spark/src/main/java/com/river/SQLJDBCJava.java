package com.river;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author river
 * @date 2019/2/22 14:02
 **/
public class SQLJDBCJava {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("sql")
                .config("spark.master","local[2]")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.130.32:3306/card?useUnicode=true&characterEncoding=utf8&useSSL=false")
                .option("dbtable", "card_card")
                .option("user", "root")
                .option("password", "123456")
                .load();

        jdbcDF.where("create_time > '2007-01-01 00:00:00'").foreach(t->  System.out.println(Thread.currentThread()+":"+t.toString()));

        jdbcDF.write().mode(SaveMode.Append).json("file:///e:/myTest/spark-demo/json-out");
    }
}
