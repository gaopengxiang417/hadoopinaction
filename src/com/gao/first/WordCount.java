package com.gao.first;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * User: wangchen.gpx
 * Date: 13-4-30
 * Time: 下午12:28
 */
public class WordCount extends Configured implements Tool{

    public static class MyMaper extends Mapper<LongWritable , Text , Text , IntWritable>{
        private static final IntWritable one = new IntWritable(1);
        private Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while (stringTokenizer.hasMoreElements()) {
                text.set(stringTokenizer.nextToken());
                context.write(text , one);
            }
        }
    }

    public static class MyReduce extends Reducer<Text , IntWritable , Text , IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key , new IntWritable(count));
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(WordCount.class);
        job.setJobName("mywordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMaper.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new WordCount(), args);
        System.exit(run);
    }
}
