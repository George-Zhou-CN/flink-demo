package com.fink.demo;

import com.fink.demo.functions.TopCategoryCounter;
import com.fink.demo.functions.TopCategoryStatisticCollector;
import com.fink.demo.functions.UvPer10Min;
import com.fink.demo.io.CategoryAsync;
import com.fink.demo.model.Category;
import com.fink.demo.model.RichUserBehavior;
import com.fink.demo.model.TopCategory;
import com.fink.demo.model.UserBehavior;
import com.fink.demo.source.UserBehaviorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

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

        AsyncFunction<UserBehavior, Tuple2<UserBehavior, Category>> function = new CategoryAsync();
        DataStream<Tuple2<UserBehavior, Category>> userBehaviorJoinCategoryDimDataStream = AsyncDataStream
                .unorderedWait(userBehaviorDataStream, function, 1000, TimeUnit.MILLISECONDS, 20)
                .setParallelism(1);

        DataStream<RichUserBehavior> richUserBehavior = userBehaviorJoinCategoryDimDataStream
                .map((MapFunction<Tuple2<UserBehavior, Category>, RichUserBehavior>) value -> {
                    UserBehavior userBehavior = value.f0;
                    Category category = value.f1;
                    return new RichUserBehavior(userBehavior, category);
                });

        DataStream<TopCategory> topCategoryDataStream = richUserBehavior
                .filter(userBehavior -> userBehavior.getBehavior().equals("buy"))
                .keyBy((KeySelector<RichUserBehavior, Long>) userBehavior -> userBehavior.getParentCategoryId())
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(1L))
                .evictor(CountEvictor.of(0L, true))
                .aggregate(new TopCategoryCounter(), new TopCategoryStatisticCollector())
                .name("Top Category");

        topCategoryDataStream.print();

    }
}
