package com.zeus.api.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;

/**
 * KafkaSink 是能结合Flink的check point机制，来支持端到端精确一次语义的，底层利用了kafka producer的事务机制
 */
public class KafkaSinks {
    public static void main(String[] args) throws URISyntaxException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setDefaultSavepointDirectory(new URI("https://localhost:8080/warehouse"));

        KafkaSource<String> kafkaConsume = KafkaSource.<String>builder()
                .setBootstrapServers("zeus:9090")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics(Arrays.asList("topic1", "topic2"))
                .setGroupId("zeus")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(kafkaConsume, WatermarkStrategy.noWatermarks(), "sourceName");

        SingleOutputStreamOperator<String> filterStream = kafkaStream.filter(Objects::nonNull);

        KafkaSink<String> kafkaProduces = KafkaSink.<String>builder()
                .setBootstrapServers("zeus:9090")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic3")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("duo")
                .build();


        filterStream.sinkTo(kafkaProduces);
    }
}
