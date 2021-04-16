package com.atguigu.partition;

import com.atguigu.writable.FlowBean;
import com.atguigu.writable.FlowDriver;
import com.atguigu.writable.FlowMapper;
import com.atguigu.writable.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName FlowPartDriver
 * @Author Kurisu
 * @Description
 * @Date 2020-11-18 14:30
 * @Version 1.0
 **/
public class FlowPartDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowPartDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //---------------------------------
        job.setNumReduceTasks(5);
        job.setPartitionerClass(Partition.class);
        //---------------------------------

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("d:/output"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
