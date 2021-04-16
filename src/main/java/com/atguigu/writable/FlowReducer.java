package com.atguigu.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName FlowReducer
 * @Author Kurisu
 * @Description
 * @Date 2020-11-17 15:07
 * @Version 1.0
 **/
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean flow = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        flow.setFlow(upFlow,downFlow);
        context.write(key,flow);
    }
}
