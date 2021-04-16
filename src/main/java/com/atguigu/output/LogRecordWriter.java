package com.atguigu.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName LogRecordWriter
 * @Author Kurisu
 * @Description
 * @Date 2020-11-19 17:06
 * @Version 1.0
 **/
public class LogRecordWriter extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream dos;
    private FSDataOutputStream dosOther;
    public LogRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统对象 ----------hadoop开流------------
            FileSystem fs = FileSystem.get(job.getConfiguration());
            String outparent = job.getConfiguration().get(FileOutputFormat.OUTDIR);
            //用文件系统对象创建两个输出流对应不同的目录
            dos = fs.create(new Path(outparent+"/atguigu.txt"));
            dosOther = fs.create(new Path(outparent+"/other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String s = value.toString();
        if(s.contains("atguigu")){
            dos.write((s+"\n").getBytes());
        }else{
            dosOther.write((s+"\n").getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(dos);
        IOUtils.closeStream(dosOther);
    }
}
