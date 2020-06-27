package com.fink.demo;

import com.fink.demo.functions.UvPer10Min;
import com.fink.demo.model.UserBehavior;
import com.fink.demo.source.UserBehaviorSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/26 2:16 下午
 * Desc: 统计一天每10分钟用户的累计用户数
 **/
public class CumulativeUvJob {

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
        DataStream<UvPer10Min> uvPer10MinDataStream = userBehaviorSource
                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1L)))
                .trigger(CountTrigger.of(1L))
                .evictor(CountEvictor.of(0L, true))
                .process(new ProcessAllWindowFunction<UserBehavior, UvPer10Min, TimeWindow>() {
                    private transient MapState<String, String> userIdState;
                    private transient ValueState<Long> uvCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        MapStateDescriptor<String, String> userIdDescriptor = new MapStateDescriptor<>("userId", String.class, String.class);
                        ValueStateDescriptor<Long> uvDescriptor = new ValueStateDescriptor("uv", Long.class);
                        this.userIdState = getRuntimeContext().getMapState(userIdDescriptor);
                        this.uvCountState = getRuntimeContext().getState(uvDescriptor);
                    }

                    @Override
                    public void process(Context context, Iterable<UserBehavior> elements, Collector<UvPer10Min> out) throws Exception {
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

                            out.collect(new UvPer10Min(userBehavior.get10Hour(), uvCount + curUvCount));
                        }

                        this.uvCountState.update(uvCount + curUvCount);
                    }
                });

        // appStatisticsSource.print();

        DataStream<UvPer10Min> cumulativeUvDateStream = uvPer10MinDataStream
                .keyBy((KeySelector<UvPer10Min, String>) uvPer10Min -> uvPer10Min.getTime())
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
                        out.collect(new UvPer10Min(key, maxUv));
                    }
                });

//        result.print();

        // 写入ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
        ElasticsearchSink.Builder<UvPer10Min> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<UvPer10Min>() {
                    public IndexRequest createIndexRequest(UvPer10Min uvPer10Min) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("time_str", uvPer10Min.getTime());
                        row.put("uv", uvPer10Min.getUv());

                        return Requests.indexRequest()
                                .index("cumulative_uv")
                                .source(row);
                    }

                    @Override
                    public void process(UvPer10Min uvPer10Min, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(uvPer10Min));
                    }
                }
        );
        // 关闭ES写入缓存
        esSinkBuilder.setBulkFlushMaxActions(1);
        cumulativeUvDateStream.addSink(esSinkBuilder.build());

        env.execute("Cumulative UV");
    }
}
