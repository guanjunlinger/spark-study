package com.study.spark.sql.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class reflection {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL file")
                .getOrCreate();

        JavaRDD<String> javaRDD = sparkSession.read().textFile("E:\\project\\spark-study\\src\\main\\resources\\person.txt").javaRDD();
        javaRDD.map(line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(parts[1].trim());
            return person;
        });
        Dataset<Row> dataSet = sparkSession.createDataFrame(javaRDD, Person.class);
        dataSet.show();
    }

}
