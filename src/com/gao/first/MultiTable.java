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
import java.util.Iterator;

/**
 * User: wangchen.gpx
 * Date: 13-5-16
 * Time: 下午7:45
 */
public class MultiTable {
    public static class MultiMapper extends Mapper<Object , Text ,Text ,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //ignore the first column
            String str = value.toString();
            if(str.contains("factoryname") || str.contains("address"))
                return;
            String[] strings = str.split(" ");
            if(strings.length != 2)
                return;
            boolean isleft = false;
            for (int i = 0; i < strings[0].length(); i++) {
                if (strings[0].charAt(i) < '0' || strings[0].charAt(i) > '9') {
                    isleft = true;
                    break;
                }
            }
            if(isleft)
                context.write(new Text(strings[1]) , new Text("1" + strings[0]));
            else
                context.write(new Text(strings[0]) , new Text("2"+strings[1]));
        }
    }

    public static class MultiReducer extends Reducer<Text ,Text ,Text ,Text>{
        int factoryyCount = 0;
        int addressCount = 0;
        String[] facotrys = new String[10];
        String[] addresses = new String[10];

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text next = iterator.next();
                String result = next.toString();
                //进行左右表的区分
                if(result.charAt(0) == '1') {
                    //左表
                    facotrys[factoryyCount] = result.substring(1);
                    factoryyCount++;
                }else {
                    addresses[addressCount] = result.substring(1);
                    addressCount++;
                }
            }

            if (factoryyCount != 0 && addressCount != 0) {
                for (int i = 0; i < factoryyCount; i++) {
                    for (int j = 0; j < addressCount; j++) {
                        context.write(new Text(facotrys[i]), new Text(addresses[j]));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        if (genericOptionsParser.getRemainingArgs().length != 3) {
            System.err.println("parameter should be two");
            System.exit(3);
        }

        Job job = new Job();
        job.setJobName("multitable job?");
        job.setJarByClass(MultiTable.class);
        job.setMapperClass(MultiMapper.class);
        job.setReducerClass(MultiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]),new Path(args[1]));
        FileOutputFormat.setOutputPath(job , new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
