package com.river;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author river
 */
public class JavaSparkStreamingWordCountApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaPairDStream<String, Integer> rst = lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x-> new Tuple2<>(x,1))
                .reduceByKey((x,y)->x+y);

        System.out.println("============================>");
        int[] pp = new int[1];
        rst.foreachRDD(t->{
            System.out.println("==========>"+pp[0]++);
//            t.foreach(x->{
//                System.out.println(x._1 + " : " + x._2);
//            });
        });

        rst.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
