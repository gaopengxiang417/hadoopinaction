package com.gao.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * User: wangchen.gpx
 * Date: 13-5-11
 * Time: 下午1:22
 */
public class MyClearDouble {
    public static class MyClearMapper extends Mapper<Object ,Text , Text ,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text resultKey = value;
            context.write(resultKey,value);
        }
    }

    public static class MyClearReducer extends Reducer<Text ,Text ,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.out.println("usage:<input><output>");
            System.exit(2);
        }
        Job job = new Job(configuration);
        job.setJarByClass(MyClearDouble.class);
        job.setJobName("clear double");
        job.setMapperClass(MyClearMapper.class);
        job.setReducerClass(MyClearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.out.println(job.waitForCompletion(true) ? 1 : 0);

    }
}
