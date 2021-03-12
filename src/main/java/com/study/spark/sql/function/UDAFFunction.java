package com.study.spark.sql.function;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class UDAFFunction {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL UDF scalar ")
                .getOrCreate();
        spark.udf().register("myAverage", new MyUDAF());

        Dataset<Row> df = spark.read().json("examples/src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }

    public static class MyUDAF extends UserDefinedAggregateFunction {
        private StructType inputSchema;
        private StructType bufferSchema;

        public MyUDAF() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.DoubleType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.DoubleType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        public StructType inputSchema() {
            return inputSchema;
        }

        public StructType bufferSchema() {
            return bufferSchema;
        }

        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        public boolean deterministic() {
            return true;
        }

        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0D);
            buffer.update(1, 0D);
        }

        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                double updateSum = buffer.getDouble(0) + input.getDouble(0);
                double updateCount = buffer.getDouble(1) + 1;
                buffer.update(0, updateSum);
                buffer.update(1, updateCount);
            }
        }

        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            double mergeSum = buffer1.getDouble(0) + buffer2.getDouble(0);
            double mergeCount = buffer1.getDouble(1) + buffer2.getDouble(1);
            buffer1.update(0, mergeSum);
            buffer1.update(1, mergeCount);

        }

        public Double evaluate(Row buffer) {
            return buffer.getDouble(0) / buffer.getDouble(1);
        }
    }
}
