package com.atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName RJMapper
 * @Author Kurisu
 * @Description
 * @Date 2020-11-20 10:20
 * @Version 1.0
 **/
public class RJMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    private TableBean bean = new TableBean();
    private Text k = new Text();
    private String filename;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //id	pid	amount
        //1001	01	1
        //pid	pname
        //01	小米
        String[] fields = value.toString().split("\t");
        if(filename.contains("order")){
            bean.setId(fields[0]);
            bean.setPid(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            //什么都不写要置空，不然序列化报错
            bean.setPname("");
            bean.setFlag("o");
        }else if(filename.contains("pd")){
            bean.setId("");
            bean.setAmount(0);
            bean.setPid(fields[0]);
            bean.setPname(fields[1]);
            bean.setFlag("p");
        }
        k.set(bean.getPid());
        context.write(k,bean);
    }
}
