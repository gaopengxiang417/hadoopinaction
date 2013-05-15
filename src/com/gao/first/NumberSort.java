package com.gao.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * User: wangchen.gpx
 * Date: 13-5-15
 * Time: 下午8:53
 */
public class NumberSort  {

    public static class SortMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable intWritable = new IntWritable();
            String str = value.toString();
            intWritable.set(Integer.valueOf(str));
            context.write(intWritable,new IntWritable(1));
        }
    }

    public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private static IntWritable numberCount = new IntWritable(1);
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(numberCount , key);
                numberCount.set(numberCount.get() + 1);
            }
        }
    }
    public static class SortParition extends Partitioner<IntWritable,IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int maxnumber = 23304;
            int bound = maxnumber / numPartitions + 1;
            int keynumber = key.get();
            for (int i = 1; i <= numPartitions; i++) {
                if (keynumber < bound && keynumber >= bound * (i - 1)) {
                    return i-1;
                }
            }
            return -1;
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        if (genericOptionsParser.getRemainingArgs().length != 2) {
            System.err.println("argument length should be two");
            System.exit(2);
        }

        Job job = new Job(configuration);
        job.setJarByClass(NumberSort.class);
        job.setJobName("sortjob");
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(SortParition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
