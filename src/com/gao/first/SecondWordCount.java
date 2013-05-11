package com.gao.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * User: wangchen.gpx
 * Date: 13-5-11
 * Time: 上午9:35
 */
public class SecondWordCount {
    public static class MySecondMapper extends Mapper<Object ,Text , Text , IntWritable>{
        public static final IntWritable one = new IntWritable(1);
        private Text text = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                String s = stringTokenizer.nextToken();
                text.set(s);
                context.write(text,one);
            }
        }
    }

    public static class MySecondReducer extends Reducer<Text , IntWritable , Text , IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable intWritable = new IntWritable();
            for (IntWritable value : values) {
                sum += value.get();
            }
            intWritable.set(sum);
            context.write(key , intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        String[] remainingArgs = genericOptionsParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.out.println("usage: word count <in><out>");
            System.exit(2);
        }

        Job job = new Job(configuration);
        job.setJobName("word count");
        job.setJarByClass(SecondWordCount.class);
        job.setMapperClass(MySecondMapper.class);
        job.setCombinerClass(MySecondReducer.class);
        job.setReducerClass(MySecondReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
