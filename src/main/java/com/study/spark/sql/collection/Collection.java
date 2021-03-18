package com.study.spark.sql.collection;

import com.study.spark.sql.rdd.Person;
import org.apache.spark.sql.*;

import java.util.Collections;

import static org.apache.spark.sql.functions.col;

public class Collection {


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("datasource Collection")
                .getOrCreate();

        Person person = new Person();
        person.setName("Andy");
        person.setAge("32");

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = sparkSession.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.where(col("name").notEqual("jun")).show();

    }
}
