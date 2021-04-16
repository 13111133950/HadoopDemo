package com.my;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName TopNMapper
 * @Author Kurisu
 * @Description
 * @Date 2020-11-20 18:41
 * @Version 1.0
 **/
public class TopNMapper extends Mapper<LongWritable,Text, Text,IntWritable> {
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,v);
    }
}
