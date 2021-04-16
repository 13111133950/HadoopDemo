package com.atguigu.partsort;

import com.atguigu.sort.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName Partition
 * @Author Kurisu
 * @Description
 * @Date 2020-11-18 14:31
 * @Version 1.0
 **/
public class Partition extends Partitioner<FlowBean,Text > {

    public int getPartition(FlowBean flowBean,Text text, int numPartitions) {
        int part = 0;
        String s = text.toString().substring(0, 3);
        switch (s) {
            case "136":
                part = 0;
                break;
            case "137":
                part = 1;
                break;
            case "138":
                part =2;
                break;
            case "139":
                part =3;
                break;
            default:
                part = 4;
        }
        return part;
    }
}
