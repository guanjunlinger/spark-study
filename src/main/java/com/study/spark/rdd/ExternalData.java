package com.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

/**
 * spark-submit --class com.study.spark.rdd.ExternalData spark-study-1.0-SNAPSHOT.jar
 */
public class ExternalData {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ExternalData").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:\\project\\spark-study\\src\\main\\resources\\word-count.txt");
        JavaPairRDD<String, Integer> javaPairRDD =
                distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                        .mapToPair(s -> new Tuple2(s, 1));
        javaPairRDD = javaPairRDD.reduceByKey((a,b)->a+b);
        System.out.println(javaPairRDD.collect());
    }
}
