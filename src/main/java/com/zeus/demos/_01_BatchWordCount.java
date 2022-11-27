package com.zeus.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
/**
 * 通过读取指定文件得到数据集
 * 统计数据集中出现的单词及其个数，输出最终结果
 */
public class _01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        //获取批处理执行环境(只输出最终处理结果)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取dataset  如果不写最后文件名，默认读取路径下所有文件
        DataSource<String> dataSet = env.readTextFile("src/main/data/_01_BatchWordCountData.txt");
        //处理
        //切割输出元组--分组--聚合
        dataSet.flatMap(new myBatchFlatMap())
                .groupBy(0)
                .sum(1)
                //批处理不需要execute()，遇到行动算子就会触发任务执行
                .print();
    }
}
/**
 * 切割数据，输出元组
 */
class myBatchFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split(",");
        for (String word : words) {
            collector.collect(Tuple2.of(word,1));
        }
    }
}