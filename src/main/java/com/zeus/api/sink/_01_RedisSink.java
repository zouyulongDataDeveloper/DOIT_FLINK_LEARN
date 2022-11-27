package com.zeus.api.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class _01_RedisSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        DataStream<String> stream = env.socketTextStream("zeus",9999);
        stream.addSink(new RedisSink<String>(conf, new RedisExampleMapper()));
    }
    static class RedisExampleMapper implements RedisMapper<String> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return null;
        }

        @Override
        public String getKeyFromData(String data) {
            return null;
        }

        @Override
        public String getValueFromData(String data) {
            return null;
        }
    }

}
