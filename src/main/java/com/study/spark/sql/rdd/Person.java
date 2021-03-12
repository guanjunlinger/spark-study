package com.study.spark.sql.rdd;

import lombok.Data;

import java.io.Serializable;

@Data
public class Person implements Serializable {
    private String name;
    private String age;
}
