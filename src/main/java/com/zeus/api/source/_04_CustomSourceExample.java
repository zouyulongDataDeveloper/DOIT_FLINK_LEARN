package com.zeus.api.source;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *自定义source
 *本质上就是定义一个类，实现SourceFunction或继承RichSourceFunction；但这种数据源都是单并行度算子，显式的改变算子并行度会报错
 * 若想实现多并行度的数据源，需要实现ParallelSourceFunction或继承RichParallelSourceFunction（可以显式的修改算子并行度）；
 *
 * SourceFunction的本质是接口
 * RichSourceFunction的本质是抽象类  其中声明周期方法继承自AbstractRichFunction，
 * 而AbstractRichFunction又通过实现RichFunction接口获得声明周期方法和Flink运行时上下文
 */
public class _04_CustomSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<AdInfo> myDataStream = env.addSource(new myRichSource());
        SingleOutputStreamOperator<AdInfo> stream = myDataStream.filter(Objects::nonNull);
        SingleOutputStreamOperator<String> jsonStream = stream.map(JSON::toJSONString);
        jsonStream.print();

        env.execute();

    }
}

/**
 * 自定义source继承RichSourceFunction
 */
class myRichSource extends RichSourceFunction<AdInfo>{
    //设置取消任务标签
    volatile boolean flag = true;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public void run(SourceContext<AdInfo> sourceContext) throws Exception {
        AdInfo adInfo = new AdInfo();
        String[] eventTypes = {"click","show","active","sign"};
        HashMap<String, String> eventInfo = new HashMap<>();
        while (flag){
            adInfo.setEventTime(System.currentTimeMillis());
            adInfo.setEventId(RandomUtils.nextLong(1,1000));
            adInfo.setEventType(eventTypes[RandomUtils.nextInt(0,eventTypes.length)].toUpperCase());
            eventInfo.put(RandomStringUtils.randomAlphabetic(2),RandomStringUtils.randomAlphabetic(2));
            adInfo.setEventInfo(eventInfo);
            sourceContext.collect(adInfo);
            eventInfo.clear();
            //产生数据频率降低
            Thread.sleep(RandomUtils.nextLong(1000,2000));
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        System.out.println("数据源关闭……");
    }
}


/**
 * pojo类
 * 模拟广告点击事件
 */
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
@Setter
class AdInfo{
    private Long eventTime;
    private Long eventId;
    private String eventType;
    private Map<String,String> eventInfo;
}