package com.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @ClassName TestCompression
 * @Author Kurisu
 * @Description
 * @Date 2020-11-23 18:53
 * @Version 1.0
 **/
public class TestCompression {
    public static void main(String[] args) throws IOException {
//        compress("d:/1.txt", GzipCodec.class);
        decompress("d:/1.txt.gz");
    }
    public static void compress(String path, Class<? extends CompressionCodec> codeclass) throws IOException {
        Configuration configuration = new Configuration();
        //通过反射创建压缩类
        CompressionCodec Codec = ReflectionUtils.newInstance(codeclass, configuration);
        FileSystem fs = FileSystem.get(configuration);
        //开流
        FSDataInputStream fis = fs.open(new Path(path));
        FSDataOutputStream fos = fs.create(new Path(path + Codec.getDefaultExtension()));
        //包装输出类
        CompressionOutputStream cos = Codec.createOutputStream(fos);
        //流对拷
        IOUtils.copyBytes(fis,cos,8192);
        //关流
        IOUtils.closeStreams(fis,cos,fos);
    }
    public static void decompress(String path) throws IOException {
        Configuration configuration = new Configuration();
        //通过工厂模式创建压缩类
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = factory.getCodec(new Path(path));
        //开流
        FileSystem fs = FileSystem.get(configuration);
        //开流
        FSDataInputStream fis = fs.open(new Path(path));
        FSDataOutputStream fos = fs.create(new Path(path.substring(0,path.length()-codec.getDefaultExtension().length())));
        //包装输入类
        CompressionInputStream cis = codec.createInputStream(fis);
        //流对拷
        IOUtils.copyBytes(cis,fos,8192);
        //关流
        IOUtils.closeStreams(cis,fos,fos);
    }
}

