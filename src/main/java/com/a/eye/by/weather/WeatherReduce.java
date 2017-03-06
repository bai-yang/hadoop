package com.a.eye.by.weather;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReduce extends Reducer<Weather, DoubleWritable, Text, NullWritable> {

    @Override
    protected void reduce(Weather arg0, Iterable<DoubleWritable> arg1,
            Reducer<Weather, DoubleWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {

        int i = 0;
        for (DoubleWritable hot : arg1) {
            i++;
            String msg = arg0.getYear() + "\t" + arg0.getMonth() + "\t" + hot.get();
            arg2.write(new Text(msg), NullWritable.get());
            if (i == 3) {
                break;
            }
        }

    }

}
