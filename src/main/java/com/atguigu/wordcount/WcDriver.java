package com.atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        args = new String[]{"input2","output"};
//
//        //—————————————————————————用本地jar包提交至集群————————————————————————
//        Configuration configuration = new Configuration();
//        //设置HDFS NameNode的地址
//        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
//        // 指定MapReduce运行在Yarn上
//        configuration.set("mapreduce.framework.name","yarn");
//        // 指定mapreduce可以在远程集群运行
//        configuration.set("mapreduce.app-submission.cross-platform","true");
//        //指定Yarn resourcemanager的位置
//        configuration.set("yarn.resourcemanager.hostname","hadoop103");
//        //—————————————————————————用本地jar包提交至集群————————————————————————
//
//        Job job = Job.getInstance(configuration);
//        //—————————————————————————用本地jar包提交至集群————————————————————————
//        job.setJar("F:\\big data\\projects\\WordCountDemo\\target\\WordCountDemo-1.0-SNAPSHOT.jar");
//        //—————————————————————————用本地jar包提交至集群————————————————————————
////        job.setJarByClass(WcDriver.class);
//        job.setMapperClass(WcMaper.class);
//        job.setReducerClass(WcReducer.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        //6. 提交任务
//        boolean b = job.waitForCompletion(true);
//
//        System.exit(b ? 0 : 1);

        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WcDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WcMaper.class);
        job.setReducerClass(WcReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
