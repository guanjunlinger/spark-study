package com.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Join {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Join").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:/project/sparkstudy/src/main/resources/word-count" + ".txt");
        JavaPairRDD<String, Integer> javaPairRDD =
                distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                        .mapToPair(s -> new Tuple2(s, 1));

        List<String> list = Arrays.asList("Are", "you", "our");
        JavaPairRDD<String, Integer> other =
                sc.parallelize(list).mapToPair(s -> new Tuple2<>(s, 1));

        System.out.println(javaPairRDD.join(other).collect());
    }
}
