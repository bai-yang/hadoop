package com.a.eye.by.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyRunJob {

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        try {

            FileSystem fs = FileSystem.get(conf);

            Job job = Job.getInstance(conf);
            job.setJarByClass(MyRunJob.class);

            job.setJobName("weather");
            job.setMapperClass(WeatherMapper.class);
            job.setReducerClass(WeatherReduce.class);

            job.setPartitionerClass(MyPartition.class);
            job.setSortComparatorClass(MySort.class);
            job.setGroupingComparatorClass(MyGroup.class);

            job.setNumReduceTasks(3);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job, new Path("/usr/input/weather"));

            Path outpath = new Path("/usr/output/weather");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("work success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
