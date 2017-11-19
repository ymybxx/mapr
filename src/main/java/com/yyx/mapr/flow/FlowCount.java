package com.yyx.mapr.flow;

import com.yyx.mapr.test.WordCountDriver;
import com.yyx.mapr.test.WordCountMapper;
import com.yyx.mapr.test.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {

    static class FlowCountMapper extends Mapper <LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取一行
            String line = value.toString();
            //切分字段
            String[] words = line.split("\t");
            //手机号
            String phoneNum = words[1];
            //上下行流量
            long upFlow = Long.parseLong(words[words.length - 3]);
            long dFlow = Long.parseLong(words[words.length - 2]);

            context.write(new Text(phoneNum), new FlowBean(upFlow, dFlow));
        }
    }


    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;

            for (FlowBean flowBean : values
                    ) {
                sum_upFlow += flowBean.getUpFlow();
                sum_downFlow += flowBean.getdFlow();
            }

            FlowBean reBean = new FlowBean(sum_upFlow, sum_downFlow);

            context.write(key, reBean);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);


        job.setJarByClass(FlowCount.class);
        //设置调用类
        job.setMapperClass(FlowCountMapper.class);

        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


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
