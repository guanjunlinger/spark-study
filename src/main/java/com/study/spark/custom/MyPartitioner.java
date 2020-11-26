package com.study.spark.custom;

import lombok.Data;
import org.apache.spark.Partitioner;

@Data
public class MyPartitioner extends Partitioner {

    private int partitioners;

    public MyPartitioner(int partitioners) {
        this.partitioners = partitioners;
    }

    @Override
    public int numPartitions() {
        return partitioners;
    }

    @Override
    public int getPartition(Object o) {
        if (o == null) {
            return 0;
        }
        return o.hashCode() % numPartitions();
    }
}
