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
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * User: wangchen.gpx
 * Date: 13-5-1
 * Time: 下午12:36
 */
public class ScoreProcess extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable , Text , Text , IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println(line);

            StringTokenizer stringTokenizer = new StringTokenizer(line,"\n");
            while (stringTokenizer.hasMoreElements()) {
                String token = stringTokenizer.nextToken();
                StringTokenizer stringTokenizer1 = new StringTokenizer(token);
                String name = stringTokenizer1.nextToken();
                String score = stringTokenizer1.nextToken();

                Text text = new Text(name);
                int scoreInt = Integer.parseInt(score);
                context.write(text , new IntWritable(scoreInt));
            }

        }
    }

    public static class MyReducer extends Reducer<Text , IntWritable , Text , IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                IntWritable next = iterator.next();
                sum += next.get();
                count++;
            }
            int average = sum / count;
            context.write(key , new IntWritable(average));
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(ScoreProcess.class);
        job.setJobName("count score");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new ScoreProcess(), args);
        System.exit(run);
    }
}
