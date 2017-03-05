package com.a.eye.by.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class WordCountRunJob {

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        try {

            FileSystem fs = FileSystem.get(conf);

            Job job = Job.getInstance(conf);

            job.setJarByClass(WordCountRunJob.class);
            job.setJobName("wc");
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path("/usr/output/wc"));

            Path outpath = new Path("/usr/output/wc");

            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("work success");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
