package com.zeus.api.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 基于文件的source，本质上就是使用指定的FileInputFormat组件读取数据，可以指定
 * TextInputFormat
 * CsvInputFormat
 * BinaryInputFormat等格式
 * 底层都是  ContinuousFileMonitoringFunction，这个类继承了  RichSourceFunction，都是非并行的source
 */
public class _02_FromFile {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取文件获得数据流
        //FileProcessingMode.PROCESS_ONCE 监视读取文件，只读取处理一次，结束退出
        //FileProcessingMode.PROCESS_CONTINUOUSLY 监视读取文件，文件内容发生改变后，从头开始读文件处理，重复消费问题
        String path = "src/main/data/_01_BatchWordCountData.txt";
        DataStreamSource<String> stream1 = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_ONCE, 10000);

        //指定读取方式
        DataStreamSource<String> stream2 = env.readTextFile(path);

        stream1.print("Stream1");
        stream2.print("Stream2");

        env.execute();

    }
}
