package com.study.spark.custom;

import lombok.Data;
import org.apache.spark.util.AccumulatorV2;

@Data
public class MyAccumulator extends AccumulatorV2<String, Long> {

    private Long value = 0L;

    @Override
    public boolean isZero() {
        return value.equals(0L);
    }

    @Override
    public AccumulatorV2<String, Long> copy() {
        MyAccumulator accumulatorV2 = new MyAccumulator();
        accumulatorV2.setValue(value.longValue());
        return accumulatorV2;
    }

    @Override
    public void reset() {
        value = 0L;
    }

    @Override
    public void add(String s) {
        value++;

    }

    @Override
    public void merge(AccumulatorV2<String, Long> accumulatorV2) {
        value += ((MyAccumulator) accumulatorV2).getValue();
    }

    @Override
    public Long value() {
        return value;
    }
}
