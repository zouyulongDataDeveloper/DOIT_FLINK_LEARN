package com.zeus.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 这些获取数据源的方式，一般只会在测试的时候使用
 */
public class _01_FromTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从集合获取数据源  三种方式都是以单并行度生成数据源，并行度无法改变
        DataStreamSource<String> numStream1 = env.fromElements("a", "b", "c", "d", "e", "f");
        DataStreamSource<String> numStream2 = env.fromCollection(Arrays.asList("a", "b", "c", "d", "e", "f"));
        //该方法有很多重载的方法，可以指定分隔符，设置最大重新连接次数
        DataStreamSource<String> numStream3 = env.socketTextStream("zeus", 9999);
        //transfromations
        numStream1.map(String :: toUpperCase).print("numStream1Sink");
        numStream2.map(String :: toUpperCase).print("numStream2Sink");
        numStream3.map(String :: toLowerCase).print("numStream3Sink");

        env.execute();
    }
}
