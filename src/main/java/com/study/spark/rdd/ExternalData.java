package com.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Arrays;

public class ExternalData {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ExternalData").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:/project/sparkstudy/src/main/resources/word-count" +
                ".txt");
        System.out.println(distFile.flatMap(s -> Arrays.asList(s.split(" ")).
                iterator()).countByValue());
    }
}
