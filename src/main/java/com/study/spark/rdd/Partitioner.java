package com.study.spark.rdd;

import com.study.spark.custom.MyPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class Partitioner {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Partitioner").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("E:/project/sparkstudy/src/main/resources/word-count" +
                ".txt");
        JavaPairRDD<String, Integer> javaPairRDD =
                distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                        .mapToPair(s -> new Tuple2(s, 1)).partitionBy(new MyPartitioner(20));

        javaPairRDD.foreachPartition(tuple2Iterator -> {
            while (tuple2Iterator.hasNext()) {
                System.out.println(tuple2Iterator.next());
            }

        });
    }
}
