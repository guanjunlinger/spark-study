package com.study.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Checkpoint {

    public static void main(String[] args) throws InterruptedException {
        String checkpointDirectory = "E:\\project\\spark-study\\src\\main\\resources\\";
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, () -> {
            SparkConf conf = new SparkConf();
            conf.setAppName("wordCount").setMaster("local[3]");
            JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(60));
            jsc.checkpoint(checkpointDirectory);
            return jsc;

        });
        JavaRDD<Integer> javaRDD = ssc.sparkContext().parallelize(Arrays.asList(1, 2, 3
                , 4, 5));
        Queue<JavaRDD<Integer>> queue = new LinkedList<>();
        queue.add(javaRDD);
        JavaDStream<Integer> lines = ssc.queueStream(queue);

        lines.foreachRDD(rdd -> System.out.println(rdd.reduce((a, b) -> a + b)));
        ssc.start();
        ssc.awaitTermination();
    }
}
