package com.study.spark.rdd;

import com.study.spark.custom.MyPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * spark-submit --class com.study.spark.Partitioner spark-study-1.0-SNAPSHOT.jar
 */
public class Partitioner {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Partitioner").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:/project/spark-study/src/main/resources/word-count.txt");
        JavaPairRDD<String, Integer> javaPairRDD =
                distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                        .mapToPair(s -> new Tuple2(s, 1)).partitionBy(new MyPartitioner(10));

        javaPairRDD.mapPartitionsWithIndex((v1, v2) -> {
            ArrayList<String> result = new ArrayList<>();
            while (v2.hasNext()){
                result.add(v1+"_"+v2.next());
            }
            return result.iterator();
        },true).foreach(s -> System.out.println(s));
    }
}
