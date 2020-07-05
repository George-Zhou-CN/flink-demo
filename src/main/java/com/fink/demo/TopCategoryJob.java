package com.fink.demo;

import com.fink.demo.functions.TopCategoryCounter;
import com.fink.demo.functions.TopCategoryStatisticCollector;
import com.fink.demo.io.CategoryAsync;
import com.fink.demo.model.Category;
import com.fink.demo.model.RichUserBehavior;
import com.fink.demo.model.TopCategory;
import com.fink.demo.model.UserBehavior;
import com.fink.demo.sink.ElasticSearchSink;
import com.fink.demo.source.UserBehaviorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.elasticsearch.action.update.UpdateRequest;

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

        DataStream<Tuple2<UserBehavior, Category>> userBehaviorJoinCategoryDimDataStream = AsyncDataStream
                .unorderedWait(userBehaviorDataStream, new CategoryAsync(), 1000, TimeUnit.MILLISECONDS, 20)
                .setParallelism(1);

        DataStream<RichUserBehavior> richUserBehavior = userBehaviorJoinCategoryDimDataStream
                .filter(tuple2 -> tuple2.f1 != null)
                .map((MapFunction<Tuple2<UserBehavior, Category>, RichUserBehavior>) value -> {
                    UserBehavior userBehavior = value.f0;
                    Category category = value.f1;
                    return new RichUserBehavior(userBehavior, category);
                });

        DataStream<TopCategory> topCategoryDataStream = richUserBehavior
                .filter(userBehavior -> userBehavior.getBehavior().equals("buy"))
                .keyBy((KeySelector<RichUserBehavior, Long>) userBehavior -> userBehavior.getParentCategoryId())
                .window(TumblingProcessingTimeWindows.of(Time.days(1L)))
                .trigger(CountTrigger.of(10L))
                .aggregate(new TopCategoryCounter(), new TopCategoryStatisticCollector())
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
