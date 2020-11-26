package com.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * spark-submit --class com.study.spark.rdd.ParallelCollection spark-study-1.0-SNAPSHOT.jar
 */
public class ParallelCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParallelCollection").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData.distinct().reduce((a, b) -> a + b));
    }

}
