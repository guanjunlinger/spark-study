package com.study.spark.sql.function;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class ScalarFunction {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL UDF scalar ")
                .getOrCreate();

        UserDefinedFunction strLen = udf(new StrLenFunction(), DataTypes.IntegerType);
        spark.udf().register("strLen", strLen);
        spark.sql("SELECT strLen('test')").show();
    }

    private static class StrLenFunction implements UDF1<String, Integer> {
        @Override
        public Integer call(String s) {
            return s.length();
        }
    }

}
