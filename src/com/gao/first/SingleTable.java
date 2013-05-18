package com.gao.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: wangchen.gpx
 * Date: 13-5-16
 * Time: 下午7:01
 */
public class SingleTable {
    private static int times = 0;

    public static class SingleMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String parentName = "";
            String childName = "";
            String result = value.toString();
            String leftRelation = "1";
            String rightRelation = "2";

            String[] arrays = result.split(" ");
            if (arrays.length != 2)
                return;
            if (arrays[0].compareTo("child") != 0) {
                childName = arrays[0];
                parentName = arrays[1];

                context.write(new Text(parentName), new Text(leftRelation + "+" + childName + "+" + parentName));
                context.write(new Text(childName), new Text(rightRelation + "+" + childName + "+" + parentName));
            }
        }
    }

    public static class SingleReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (times == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                times++;
            }

            int parentCount = 0;
            int childCount = 0;
            String[] parentNames = new String[10];
            String[] childNames = new String[10];
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text names = iterator.next();
                String parentName = "";
                String childName = "";

                String[] split = names.toString().split("\\+");
                if (split.length != 3)
                    continue;
                if (split[0].equals("1")) {
                    childNames[childCount] = split[1];
                    childCount++;
                }else {
                    parentNames[parentCount] = split[2];
                    parentCount++;
                }
            }

            if (parentCount != 0 && childCount != 0) {
                for (int i = 0; i < childCount; i++) {
                    for (int j = 0; j < parentCount; j++) {
                        context.write(new Text(childNames[i]) , new Text(parentNames[j]));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        if (genericOptionsParser.getRemainingArgs().length != 2) {
            System.err.println("argument should be two");
            System.exit(2);
        }

        Job job = new Job(configuration);
        job.setJobName("single table");
        job.setJarByClass(SingleTable.class);
        job.setMapperClass(SingleMapper.class);
        job.setReducerClass(SingleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
