package com.study.spark.sql.hive;

import org.apache.spark.sql.SparkSession;

public class HiveTable {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("HiveTable")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (name STRING, age STRING) USING hive");
        sparkSession.sql("LOAD DATA LOCAL INPATH 'E:\\project\\spark-study\\src\\main\\resources\\person.txt' INTO TABLE src");
        sparkSession.sql("SELECT * FROM src").show();
    }
}
