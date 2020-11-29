package com.study.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * spark-submit --class com.study.spark.streaming.WordCount spark-study-1.0-SNAPSHOT.jar
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("wordCount").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(60));
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("111", "222", "333"
                , "111", "222"));
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(javaRDD);
        JavaDStream<String> lines = ssc.queueStream(queue);

        JavaPairDStream<String, Integer> wordCounts = lines.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((a, b) -> a + b);
        wordCounts.foreachRDD(rdd -> System.out.println(rdd.collect()));
        ssc.start();
        ssc.awaitTermination();
    }
}

