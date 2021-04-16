package com.atguigu.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName MjDriver
 * @Author Kurisu
 * @Description
 * @Date 2020-11-19 22:03
 * @Version 1.0
 **/
public class MjDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        args = new String[]{"d:/mapjoin/input","d:/mapjoin/output"};

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MjDriver.class);
        job.setMapperClass(MjMapper.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI("file:///D:/mapjoin/inputcache/pd.txt"));
//        job.setReducerClass(WcReducer.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交任务
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
