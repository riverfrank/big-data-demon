package com.river.tag;

import com.google.gson.Gson;
import com.river.tag.dto.ReviewVo;
import javafx.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.util.stream.Collectors;

public class TagSparkDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("MapReduceActionDemon")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filePaht = "/Users/riverfan/mytest/spark/tag/temptags.txt";
        sc.textFile(filePaht)
                .map(t-> t.split("\t")).filter(t->t.length == 2)
                .mapToPair(t-> {
                    ReviewVo reviewVo = new Gson().fromJson(t[1],ReviewVo.class);
                    return new Tuple2<>(t[0],reviewVo);
                })
                .filter(t-> !CollectionUtils.isEmpty(t._2.getExtInfoList()))
                .map(t->{
                    return t._2.getExtInfoList().stream()
                            .filter(i->"contentTags".equals(i.getTitle()))
                            .map(i->i.getValues())
                            .flatMap(i->i.stream()).map(i->new Tuple2<>(t._1,i)).collect(Collectors.toList());
                }).flatMap(t->t.stream().iterator())
                .mapToPair(t->new Tuple2<>(t._1 + "-" + t._2 , 1))
                //.groupByKey()
                .reduceByKey((t1,t2)-> t1+t2)
                .map(t-> {
                    System.out.println(t._1.split("-")[0]);
                    System.out.println(t._1.split("-")[1]);
                    System.out.println(t._1);
                    return new Tuple3<>(t._2,t._1.split("-")[1],t._1.split("-")[0]);
                })
                .sortBy(new Function<Tuple3<Integer,String,String>, Object>() {

                    @Override
                    public Object call(Tuple3<Integer, String, String> v1) throws Exception {
                        return v1._1();
                    }
                },false,1)
                .groupBy(Tuple3::_3)

                .foreach(t-> System.out.println(t._1() + " " + t._2()));
    }

}
