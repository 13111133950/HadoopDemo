package com.atguigu.partsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName FlowMapper
 * @Author Kurisu
 * @Description
 * @Date 2020-11-17 15:07
 * @Version 1.0
 **/
public class FlowMapper extends Mapper<LongWritable,Text, FlowBean,Text> {
    FlowBean flow = new FlowBean();
    Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        flow.setFlow(Long.parseLong(fileds[1]),Long.parseLong(fileds[2]));
        phone.set(fileds[0]);
        context.write(flow,phone);
    }
}
