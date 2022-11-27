package com.zeus.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink流批一体
 * 写一套流式处理的代码，只需要更改运行模式，Flink就可以更改底层的运算方式为批处理，所以没有必要单独学习批处理api
 * 批处理和流处理在执行相同的计算逻辑时，底层的计算方法可能存在差异，比如wordCount中，批处理涉及到预聚合
 *
 * 如果不设置执行模式为批处理，计算的时候会将数据集当作有界流处理，不断输出当时最新的计算结果；而改成批处理后，只输出最终的结果
 *
 * 一般情况下使用流处理，比如需要重跑补数，结果验证的时候会用到批处理
 */
public class _01_StreamBatchWordCount {
    public static void main(String[] args) throws Exception{
        //创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //使用流式执行环境读取指定文件   如果不写最后文件名，默认读取路径下所有文件
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = env.readTextFile("src/main/data/_01_BatchWordCountData.txt")
                .flatMap(new myStreamingFlatMap())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                //此处可以使用元组列名 f0 f1...  也可使用下标
                .sum(1);

        resultStream.print("SteamBatchMode");

        env.execute("SteamBatchMode");
    }
}
