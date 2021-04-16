package com.atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @ClassName MjMapper
 * @Author Kurisu
 * @Description
 * @Date 2020-11-19 22:03
 * @Version 1.0
 **/
public class MjMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    HashMap<String,String> pdmap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件路径
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        //开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(path));
        //字符流转换为字节流
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line ="";
        //StringUtils.isnotempty   判断非空+字符串长度不为0
        while (StringUtils.isNotEmpty(line = br.readLine())){
            String[] fields = line.split("\t");
            pdmap.put(fields[0],fields[1]);
        }
        IOUtils.closeStream(br);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        //01	小米
        String s = value.toString();
        String[] fields = s.split("\t");
        String pname = pdmap.get(fields[1]);
        context.write(new Text(s+pname),NullWritable.get());
    }
}
