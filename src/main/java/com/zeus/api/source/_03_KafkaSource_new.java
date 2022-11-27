package com.zeus.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;

/**
 * 1.14版本kafka source
 * 之前的版本不支持kafka的精确一次消费，因为无法控制KafkaConsumer向kafka（consumer_offsets）提交偏移量的时机（固定时间间隔提交，若意外挂掉则偏移量不准确）
 * 新版本中，KafkaSource会把消费的偏移量保存到算子状态中，不再依赖kafka所记录的偏移量，能够真正实现精确一次消费
 *
 * old:FlinkKafkaConsumer(算子)     SourceFunction     AddSource
 * new:KafkaSource(算子)   source    FromSource
 */
public class _03_KafkaSource_new {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("zeus:9092")
                .setGroupId("zeus")
                .setTopics(Arrays.asList("topic01", "topic02"))
                //设置特殊的属性，一般属性的设置已经在新版KafkaSource中提供了
                .setProperty("auto.offest.commit", "true")
                //设置读取偏移量
                //OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 从该消费者组上次提交的偏移量处读读数据
                //子策略（重置策略）：若没找到或者该消费者组第一次出现，则从最新位置开始读数据
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //把本source算子设置成有界流，读取到指定位置后结束程序
                //一般用于补数或重跑某一段时间的历史数据
//                .setBounded(OffsetsInitializer.committedOffsets())
                //把本source算子设置成无界流，读取到指定位置后停止读取数据，程序不退出
                //应用场景：从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另一个流联合处理
//                .setUnbounded(OffsetsInitializer.committedOffsets())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource", TypeInformation.of(String.class));
        streamSource.print("kafkaSource");

        env.execute();


    }
}
