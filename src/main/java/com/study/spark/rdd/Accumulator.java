package com.study.spark.rdd;

import com.study.spark.custom.MyAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * spark-submit --class com.study.spark.rdd.Accumulator spark-study-1.0-SNAPSHOT.jar
 */
public class Accumulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:\\project\\spark-study\\src\\main\\resources\\word-count.txt")
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        MyAccumulator myAccumulator = new MyAccumulator();
        sc.sc().register(myAccumulator, "myAccumulator");
        distFile.foreach(s -> myAccumulator.add(s));
        System.out.println(myAccumulator.getValue());
    }
}
