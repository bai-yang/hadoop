package com.a.eye.by.weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartition extends HashPartitioner<Weather, DoubleWritable> {

    @Override
    public int getPartition(Weather key, DoubleWritable value, int numReduceTasks) {
        return (key.getYear() - 1949) % numReduceTasks;
    }

}
