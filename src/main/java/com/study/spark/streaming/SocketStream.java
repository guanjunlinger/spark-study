package com.study.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


/**
 * spark-submit --class com.study.spark.streaming.SocketStream spark-study-1.0-SNAPSHOT.jar
 */
public class SocketStream {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("SocketStream").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        JavaDStream<String> lines = ssc.socketTextStream("localhost", 8888);

        JavaPairDStream<String, Integer> wordCounts = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((a, b) -> a + b);
        wordCounts.foreachRDD(rdd -> System.out.println(rdd.collect()));
        ssc.start();
        ssc.awaitTermination();
    }
}

