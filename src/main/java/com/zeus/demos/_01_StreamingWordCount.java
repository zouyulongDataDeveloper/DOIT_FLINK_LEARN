package com.zeus.demos;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过socket数据源（192.168.66.66：9999），去请求一个socket服务，得到数据流
 * 统计数据流中出现的单词及其个数
 */
public class _01_StreamingWordCount {
    public static void main(String[] args) throws Exception {
        //创建流式执行环境(不断输出当前时刻最新处理结果)
        //本地执行并开启webUI本地运行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //设置算子并行度 默认为本地设备的cpu逻辑核数
        env.setParallelism(1);
        //监听指定端口获取数据流  “nc -lk 9999”
        DataStreamSource<String> zeusStream = env.socketTextStream("zeus", 9999);
        //处理数据流
        //将数据流中的数据切成单词，然后返回(word,1)，但是java中没有元组，flink单独封装了一种元组(导包时注意)
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream =  zeusStream.flatMap(new myStreamingFlatMap()).
                keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                //按照元组第二个元素聚合
                .sum("f1");

        //打印结果流到控制台
        resultStream.print("resultStream");

        //执行程序
        env.execute("WordCount");
    }

}
/**
 * 切割数据，返回元组
 * param1 输入参数
 * param2 输出参数
 */
class myStreamingFlatMap extends RichFlatMapFunction<String,Tuple2<String,Integer>>{

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split(" ");
        for (String word : words) {
            collector.collect(Tuple2.of(word,1));
        }
    }
}