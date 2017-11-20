package com.yyx.mapr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSort {
    static  class FlowSortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {
        private FlowBean flowBean = new FlowBean();
        private Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            String phoneNum = words[0];

            flowBean.set(Long.parseLong(words[1]), Long.parseLong(words[2]));
            text.set(phoneNum);

            context.write(flowBean, text);

        }
    }


    static class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }


    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);


        job.setJarByClass(FlowSort.class);
        //设置调用类
        job.setMapperClass(FlowSort.FlowSortMapper.class);

        job.setReducerClass(FlowSort.FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);



        //指定输入输入输出路径

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所用的jar包，提交给yarn去运行
        //job.submit();

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }

}
