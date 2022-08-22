package com.fink.demo;

import com.fink.demo.functions.UvPer10Min;
import com.fink.demo.model.UserBehavior;
import com.fink.demo.sink.ElasticSearchSink;
import com.fink.demo.source.UserBehaviorSource;
import com.fink.demo.util.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/26 2:16 下午
 * Desc: 统计一天每10分钟用户的累计用户数
 **/
public class CumulativeUvJob {
    /**
     * 任务主入口
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 使用时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 获取数据源
        FlinkKafkaConsumer<UserBehavior> kafkaConsumer = UserBehaviorSource.getSource();
        kafkaConsumer.setStartFromEarliest();
        DataStream<UserBehavior> userBehaviorSource = env.addSource(kafkaConsumer)
                .name("User Behavior Source");

        // 业务逻辑处理
        // 按照天分组，状态在1个小时候之后没有写入就自动清除
        DataStream<UvPer10Min> uvPer10MinDataStream = userBehaviorSource
                .keyBy(userBehavior -> DateUtils.format(userBehavior.getTs(), "yyyy-MM-dd"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<UserBehavior, UvPer10Min, String, TimeWindow>() {

                    private transient MapState<String, String> userIdState;
                    private transient ValueState<Long> uvCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        StateTtlConfig userIdTtlConf = StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.hours(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        StateTtlConfig uvTtlConf = StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.hours(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        ValueStateDescriptor<Long> uvDescriptor = new ValueStateDescriptor("uv", Long.class);
                        MapStateDescriptor<String, String> userIdDescriptor = new MapStateDescriptor<>("userId", String.class, String.class);
                        userIdDescriptor.enableTimeToLive(userIdTtlConf);
                        uvDescriptor.enableTimeToLive(uvTtlConf);
                        this.userIdState = getRuntimeContext().getMapState(userIdDescriptor);
                        this.uvCountState = getRuntimeContext().getState(uvDescriptor);
                    }

                    @Override
                    public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UvPer10Min> out) throws Exception {
                        Long uvCount = this.uvCountState.value() == null ? 0L : this.uvCountState.value();
                        Long curUvCount = 0L;
                        Iterator<UserBehavior> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            UserBehavior userBehavior = iterator.next();
                            String userId = String.valueOf(userBehavior.getUserId());
                            if (StringUtils.isBlank(userIdState.get(userId))) {
                                userIdState.put(userId, "1");
                                curUvCount++;
                            }

                            out.collect(new UvPer10Min(userBehavior.getTs(), null, uvCount + curUvCount));
                        }

                        this.uvCountState.update(uvCount + curUvCount);
                    }
                });
//        uvPer10MinDataStream.print();

        DataStream<UvPer10Min> cumulativeUvDateStream = uvPer10MinDataStream
                .keyBy((KeySelector<UvPer10Min, String>) uvPer10Min -> DateUtils.getDateAndHourAnd10Min(uvPer10Min.getTime()))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessWindowFunction<UvPer10Min, UvPer10Min, String, TimeWindow>() {
                    private transient ValueState<Long> maxUvState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        ValueStateDescriptor<Long> maxUvDescriptor = new ValueStateDescriptor("maxUv", Long.class);
                        this.maxUvState = getRuntimeContext().getState(maxUvDescriptor);
                    }

                    @Override
                    public void process(String key, Context context, Iterable<UvPer10Min> elements, Collector<UvPer10Min> out) throws Exception {
                        Long maxUv = this.maxUvState.value() == null ? 0L : this.maxUvState.value();

                        Iterator<UvPer10Min> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            UvPer10Min uvPer10Min = iterator.next();
                            if (uvPer10Min.getUv() > maxUv) {
                                maxUv = uvPer10Min.getUv();
                            }
                        }

                        this.maxUvState.update(maxUv);
                        out.collect(new UvPer10Min(null, key, maxUv));
                    }
                });

        cumulativeUvDateStream.print();

        // 写入ES
        ElasticsearchSink<UvPer10Min> esSink = ElasticSearchSink
                .buildSink((ElasticsearchSinkFunction<UvPer10Min>) (uvPer10Min, runtimeContext, requestIndexer) -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("time_str", uvPer10Min.getKey());
                    row.put("uv", uvPer10Min.getUv());

                    requestIndexer.add(Requests.indexRequest().index("cumulative_uv").source(row));
                });
        cumulativeUvDateStream.addSink(esSink);

        env.execute("Cumulative UV");
    }
}
