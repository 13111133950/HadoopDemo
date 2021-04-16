package com.atguigu.combine;

import com.atguigu.wordcount.WcMaper;
import com.atguigu.wordcount.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName WcDriver
 * @Author Kurisu
 * @Description
 * @Date 2020-11-17 11:40
 * @Version 1.0
 **/
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/cominput","d:/output"};

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WcDriver.class);
        job.setMapperClass(WcMaper.class);
        job.setReducerClass(WcReducer.class);
        //--------------
        job.setInputFormatClass(CombineTextInputFormat.class);
        //如果参数比blocksize小，则按此大小切片
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        //--------------
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交任务
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);


    }
}
