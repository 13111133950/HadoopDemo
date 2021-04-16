package com.atguigu.output;


import com.atguigu.wordcount.WcMaper;
import com.atguigu.wordcount.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Driver;

/**
 * @ClassName WcDriver
 * @Author Kurisu
 * @Description
 * @Date 2020-11-17 11:40
 * @Version 1.0
 **/
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/outinput","d:/output6"};
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(LogDriver.class);
        //mapper reducer没做事可以直接用官方
//        job.setMapperClass(LogMapper.class);
//        job.setReducerClass(LogReduce.class);
          job.setOutputFormatClass(MyOutPutFormat.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交任务
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);


    }
}
