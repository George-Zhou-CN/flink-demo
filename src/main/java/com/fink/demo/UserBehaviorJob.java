package com.fink.demo;

import com.fink.demo.functions.UserBuyCounter;
import com.fink.demo.functions.UserBuyStatisticsCollector;
import com.fink.demo.model.UserBehavior;
import com.fink.demo.model.UserBuyCount;
import com.fink.demo.source.UserBehaviorSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/25 2:16 下午
 * Desc: 统计每小时的成交量
 **/
public class UserBehaviorJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 获取数据源
        FlinkKafkaConsumer<UserBehavior> kafkaConsumer = UserBehaviorSource.getSource();
        kafkaConsumer.setStartFromEarliest();
        DataStream<UserBehavior> userBehaviorSource = env.addSource(kafkaConsumer)
                .name("User Behavior Source")
                .filter(userBehavior -> userBehavior.getBehavior().equals("buy"))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MICROSECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTs().getTime();
                    }
                });

        // 逻辑处理
        DataStream<UserBuyCount> userBuyCountSource = userBehaviorSource
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new UserBuyCounter(), new UserBuyStatisticsCollector())
                .name("User Buy Count");

        userBuyCountSource.print();

        // 写入ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
        ElasticsearchSink.Builder<UserBuyCount> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<UserBuyCount>() {
                    public IndexRequest createIndexRequest(UserBuyCount userBuyCount) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("hour", userBuyCount.getHour());
                        row.put("count", userBuyCount.getCount());

                        return Requests.indexRequest()
                                .index("buy_cnt_per_hour")
                                .source(row);
                    }

                    @Override
                    public void process(UserBuyCount userBuyCount, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(userBuyCount));
                    }
                }
        );
        // 关闭ES写入缓存
        esSinkBuilder.setBulkFlushMaxActions(1);
        userBuyCountSource.addSink(esSinkBuilder.build());

        // 提交任务
        env.execute("UserBehavior");
    }
}