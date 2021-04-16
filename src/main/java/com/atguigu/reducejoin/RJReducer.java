package com.atguigu.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName RJReducer
 * @Author Kurisu
 * @Description
 * @Date 2020-11-20 10:20
 * @Version 1.0
 **/
public class RJReducer extends Reducer<Text,TableBean,TableBean, NullWritable>{
    HashMap<String,String> list = new HashMap<>();
    TableBean tb = new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        for (TableBean bean : values) {
            if("p".equals(bean.getFlag())){
                //pid	pname
                // 01	小米
                list.put(bean.getPid(),bean.getPname());
            }else{
                TableBean tmp = new TableBean();
                try {
                    BeanUtils.copyProperties(tmp,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmp);
            }
        }
        for (TableBean bean : orderBeans) {
            if("o".equals(bean.getFlag())){
                //id	pid	amount
                // 1001	01	1
                tb.setId(bean.getId());
                tb.setAmount(bean.getAmount());
                String pname = list.get(bean.getPid());
                tb.setPname(pname);
                context.write(tb,NullWritable.get());
            }
        }
    }
}
