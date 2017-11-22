package com.yyx.mapr.join;

import com.yyx.mapr.flow.FlowBean;
import com.yyx.mapr.flow.FlowSort;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNodeInfoMXBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {

        InfoBean infoBean = new InfoBean();
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String[] fields = line.split(",");
            String fileName = inputSplit.getPath().getName();
            //通过文件名判断是哪种数据
            if (fileName.startsWith("order")) {

                infoBean.set(Integer.parseInt(fields[0]),fields[1],Integer.parseInt(fields[2]),Integer.parseInt(fields[3]),
                        "",0,0,0);
            } else {
                infoBean.set(0, "", Integer.parseInt(fields[0]), 0,
                        fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), 1);
            }
            text.set(fields[2]);
            context.write(text, infoBean);
        }
    }


    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            List<InfoBean> orderBeans = new ArrayList<InfoBean>();
            for (InfoBean infoBean : values) {
                if (infoBean.getFlag() == 1) {
                    try {
                        BeanUtils.copyProperties(pdBean,infoBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else {
                    InfoBean odBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odBean, infoBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    orderBeans.add(odBean);
                }
            }

            for (InfoBean infoBean : orderBeans) {
                infoBean.setpName(pdBean.getpName());
                infoBean.setCategoryId(pdBean.getCategoryId());
                infoBean.setPrice(pdBean.getPrice());
                context.write(infoBean,NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.framwork.name", "local");

        conf.set("fs.defaultFs","file:///");
        Job job = Job.getInstance(conf);


       // job.setJarByClass(RJoin.class);

        job.setJar("/Users/inequality/IdeaProjects/mapr/out/artifacts/mapr_jar/mapr.jar");
        job.setJarByClass(FlowSort.class);
        //设置调用类
        job.setMapperClass(RJoinMapper.class);

        job.setReducerClass(RJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);


        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);



        //指定输入输入输出路径

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所用的jar包，提交给yarn去运行
        //job.submit();

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }

}

