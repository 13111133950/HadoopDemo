package com.atguigu.wordcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WcCombiner
 * @Author Kurisu
 * @Description
 * @Date 2020-11-19 09:44
 * @Version 1.0
 **/
public class WcCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
