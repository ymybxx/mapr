package com.yyx.mapr.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 继承map类
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * map阶段的业务逻辑
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LongWritable one = new LongWritable(1);
        //获取每行的内容
        String line = value.toString();
        //切分单词
        String[] words = line.split(" ");
        //将单词输出为<word 1>
        for (String word : words) {
            context.write(new Text(word), one);
        }
    }
}
