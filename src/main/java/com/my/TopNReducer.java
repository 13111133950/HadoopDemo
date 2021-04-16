package com.my;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @ClassName TopNReducer
 * @Author Kurisu
 * @Description
 * @Date 2020-11-20 18:41
 * @Version 1.0
 **/
public class TopNReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    TreeMap<Integer,String> map = new TreeMap<>();
    Text k = new Text();
    IntWritable v = new IntWritable();
    String s = "";

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        map.put(sum,key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        NavigableMap<Integer,String> map1 = map.descendingMap();
        int topN = context.getConfiguration().getInt("TopN", 1);
        int i = 1;
        for (Map.Entry<Integer,String > stringIntegerEntry : map1.entrySet()) {
            k.set(stringIntegerEntry.getValue());
            v.set(stringIntegerEntry.getKey());
            context.write(k,v);
            i++;
            if (i>topN){
                break;
            }
        }
    }
}
