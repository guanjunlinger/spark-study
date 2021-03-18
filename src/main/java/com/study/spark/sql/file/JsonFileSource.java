package com.study.spark.sql.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonFileSource {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL file")
                .getOrCreate();

        Dataset<Row> dataSet = sparkSession.read().json("E:\\project\\spark-study\\src\\main\\resources\\person.json");
        dataSet.createOrReplaceTempView("person");
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM person where age='32'");
        sqlDF.show();

    }
}
