package com.yyx.mapr.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framwork.name", "local");
//
//        conf.set("fs.defaultFs","file:///");

        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.homtname","hadoop0");
        conf.set("fs.default","hdfs://hadoop0:8020");


        Job job = Job.getInstance(conf);

        job.setJar("/Users/inequality/IdeaProjects/mapr/out/artifacts/mapr_jar/mapr.jar");
        //job.setJarByClass(WordCountDriver.class);
        //设置调用类
        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //自定义数据分区
        //设置reducetask分区数量

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //指定输入输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所用的jar包，提交给yarn去运行
        //job.submit();

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }
}
