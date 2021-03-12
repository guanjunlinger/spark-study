package com.study.spark.sql.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Program {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL file")
                .getOrCreate();

        JavaRDD<String> javaRDD = sparkSession.read().textFile("E:\\project\\spark-study\\src\\main\\resources\\person.txt").javaRDD();
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = javaRDD.map(record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> dataset = sparkSession.createDataFrame(rowRDD, schema);
        dataset.show();
    }
}
