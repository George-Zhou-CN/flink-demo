package com.fink.demo;

import com.fink.demo.io.CategoryAsync;
import com.fink.demo.model.*;
import com.fink.demo.sink.ElasticSearchSink;
import com.fink.demo.source.UserBehaviorSource;
import com.fink.demo.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.update.UpdateRequest;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 8:00 下午
 * @Desc 统计顶级类目排行榜
 **/
public class TopCategoryJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FlinkKafkaConsumer<UserBehavior> kafkaConsumer = UserBehaviorSource.getSource();
        kafkaConsumer.setStartFromEarliest();
        DataStream<UserBehavior> userBehaviorDataStream = env.addSource(kafkaConsumer).name("User Behavior Source");

        // 通过加长超时时间和减少并发量来解决超时问题
        DataStream<Tuple2<UserBehavior, Category>> userBehaviorJoinCategoryDataStream = AsyncDataStream
                .unorderedWait(userBehaviorDataStream, new CategoryAsync(), 2000, TimeUnit.MILLISECONDS, 10)
                .setParallelism(1);

        DataStream<RichUserBehavior> richUserBehavior = userBehaviorJoinCategoryDataStream
                .filter(tuple2 -> tuple2.f1 != null)
                .map((MapFunction<Tuple2<UserBehavior, Category>, RichUserBehavior>) value -> {
                    UserBehavior userBehavior = value.f0;
                    Category category = value.f1;
                    return new RichUserBehavior(userBehavior, category);
                });

        // 利用onTime方法解决写入下游es的压力，并在当晚23:59:59点清除状态
        DataStream<TopCategory> topCategoryDataStream = richUserBehavior
                .filter(userBehavior -> userBehavior.getBehavior().equals("buy"))
                .keyBy((KeySelector<RichUserBehavior, Long>) userBehavior -> userBehavior.getParentCategoryId())
                .process(new KeyedProcessFunction<Long, RichUserBehavior, TopCategory>() {
                    private ValueState<CountWithTimestamp> countWithTimestampState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        countWithTimestampState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("countWithTimestampState", CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(RichUserBehavior value, Context ctx, Collector<TopCategory> out) throws Exception {
                        CountWithTimestamp current = countWithTimestampState.value();
                        if (current == null) {
                            current = new CountWithTimestamp(String.valueOf(value.getParentCategoryId()), value.getParentCategoryName());
                        }

                        long time = ctx.timerService().currentProcessingTime();
                        long triggerTime = DateUtils.jumpSeconds(time);
                        current.increase();
                        current.setLastModified(triggerTime);
                        countWithTimestampState.update(current);

                        ctx.timerService().registerProcessingTimeTimer(triggerTime);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopCategory> out) throws Exception {
                        CountWithTimestamp countWithTimestamp = countWithTimestampState.value();
                        out.collect(new TopCategory(Long.valueOf(countWithTimestamp.getKey()), countWithTimestamp.getName(), countWithTimestamp.getCount()));

                        // 如果等于每天的23:59:59，清除状态
                        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp / 1000), ZoneId.systemDefault());
                        if (ldt.getHour() == 23 && ldt.getMinute() == 59 && ldt.getSecond() == 59) {
                            countWithTimestampState.clear();
                        }
                    }
                })
                .name("Top Category");
        topCategoryDataStream.print();

        ElasticsearchSink<TopCategory> esSink = ElasticSearchSink
                .buildSink((ElasticsearchSinkFunction<TopCategory>) (topCategory, runtimeContext, requestIndexer) -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("category_id", topCategory.getCategoryId());
                    row.put("category_name", topCategory.getCategoryName());
                    row.put("uv", topCategory.getBuyCount());

                    UpdateRequest updateRequest = new UpdateRequest();
                    updateRequest.index("top_category").id(topCategory.getCategoryId().toString()).upsert(row).doc(row).docAsUpsert(true);
                    requestIndexer.add(updateRequest);
                });
        topCategoryDataStream.addSink(esSink);

        env.execute();
    }
}
