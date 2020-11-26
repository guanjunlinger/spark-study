package com.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * spark-submit --class com.study.spark.rdd.BroadcastVariable spark-study-1.0-SNAPSHOT.jar
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Broadcast<List<String>> broadcastVar = sc.broadcast(Arrays.asList("ni,hao"));
        JavaRDD<String> distFile = sc.textFile("E:\\project\\spark-study\\src\\main\\resources\\word-count.txt")
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        distFile.foreach(a -> {
            System.out.println(a);
            System.out.println(broadcastVar.getValue());
        });

    }
}
