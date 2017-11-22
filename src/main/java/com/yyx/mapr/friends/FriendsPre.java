package com.yyx.mapr.friends;

import com.yyx.mapr.flow.FlowBean;
import com.yyx.mapr.flow.FlowSort;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FriendsPre {


    static public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text friendText = new Text();
        Text usrText = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取每一行的好友内容
            String line = value.toString();
            String[] spil1 = line.split(":");
            //当前用户
            String usr = spil1[0];
            //用户的所有好友
            String[] friends = spil1[1].split(",");
            //以该用户为为value，好友为key,输出给map
            for (String friend:friends
                 ) {
                friendText.set(friend);
                usrText.set(usr);
                context.write(friendText, usrText);
            }
        }
    }


    static public class FriendsReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text usrText = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //将iterator转化为list,因为iterator没有get方法，无法操作下标
            List<Text> usersList = new ArrayList<Text>();
            for (Text usr : values) {
                usersList.add(usr);
            }

            for (int i = 0; i < usersList.size() - 1; i++) {
                for (int j = i + 1; j < usersList.size(); j++) {
                    //生成关系字符串如:a--b:c
                    usrText.set(usersList.get(i).toString() + "--"+usersList.get(j)+":"+key.toString());
                    context.write(usrText, NullWritable.get());
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);


        job.setJarByClass(FriendsPre.class);
        //设置调用类
        job.setMapperClass(FriendsMapper.class);

        job.setReducerClass(FriendsReducer.class);

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
