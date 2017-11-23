package com.yyx.mapr.join;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoin {


    static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> pfInfoMap = new HashMap<String,String>();
        Text reText = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream("pro.txt")));

            String line;
            while (StringUtils.isNotEmpty(line = bf.readLine())) {
                String[] fields = line.split(",");
                pfInfoMap.put(fields[0], fields[1]);
            }
            bf.close();


        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLine = value.toString();
            String orderWords[] = orderLine.split(",");
            String pid = orderWords[2];
            reText.set(orderLine + "\t" + pfInfoMap.get(pid));
            context.write(reText, NullWritable.get());
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //job.setJarByClass(MapJoin.class);
        job.setJar("/Users/inequality/IdeaProjects/mapr/out/artifacts/mapr_jar/mapr.jar");
        job.setMapperClass(MapJoinMapper.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]),true);
        }
        //指定一个文件到所有maptask运行节点工作目录
        job.addCacheFile(new URI("file:///Users/inequality/tmp/input/join/pro.txt"));

        Boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
