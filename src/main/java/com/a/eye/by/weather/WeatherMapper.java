package com.a.eye.by.weather;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<Text, Text, Weather, DoubleWritable> {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Weather, DoubleWritable>.Context context)
            throws IOException, InterruptedException {
        try {

            Date date = sdf.parse(key.toString());
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH);

            double hot = Double.parseDouble(value.toString().substring(0, value.toString().lastIndexOf("c")));

            Weather w = new Weather();
            w.setHot(hot);
            w.setMonth(month);
            w.setYear(year);

            context.write(w, new DoubleWritable(hot));

        } catch (Exception e) {

        }
    }

}
