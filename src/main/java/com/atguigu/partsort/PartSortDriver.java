package com.atguigu.partsort;

import com.atguigu.sort.FlowBean;
import com.atguigu.sort.FlowMapper;
import com.atguigu.sort.FlowReducer;
import com.atguigu.sort.SortDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName PartSortDriver
 * @Author Kurisu
 * @Description
 * @Date 2020-11-18 22:57
 * @Version 1.0
 **/
public class PartSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SortDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(com.atguigu.sort.FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("d:/output"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
