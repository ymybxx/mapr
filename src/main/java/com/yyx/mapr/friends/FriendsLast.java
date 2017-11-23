package com.yyx.mapr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendsLast {


    static class FriendsLastMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text relationText = new Text();
        Text friendText = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split(":");
            //关系字符串
            String relation = words[0];
            //共同好友
            String friend = words[1];

            //输出
            relationText.set(relation);
            friendText.set(friend);

            context.write(relationText, friendText);
        }
    }


    static class FriendsLastReducer extends Reducer<Text, Text, Text, NullWritable> {

        Text re = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer reSb = new StringBuffer(key.toString());
            reSb.append(": ");//a--b:
            for (Text friend : values) {
                reSb.append(friend.toString()).append(" ");
            }
            re.set(reSb.toString());
            context.write(re, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);


        job.setJarByClass(FriendsPre.class);

        job.setJar("/Users/inequality/IdeaProjects/mapr/out/artifacts/mapr_jar/mapr.jar");
        //设置调用类
        job.setMapperClass(FriendsLastMapper.class);

        job.setReducerClass(FriendsLastReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);



        //指定输入输入输出路径

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        //将job中配置的相关参数，以及job所用的java类所用的jar包，提交给yarn去运行
        //job.submit();

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }



}
