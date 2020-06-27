package com.fink.demo.functions;

import com.fink.demo.model.UserBuyCount;
import com.fink.demo.util.DateUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: jiazhi
 * @Date: 2020/6/24 11:47 下午
 * @Desc:
 */
public class UserBuyStatisticsCollector extends ProcessAllWindowFunction<Long, UserBuyCount, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Long> elements, Collector<UserBuyCount> out) throws Exception {
        Long count = elements.iterator().next();
        out.collect(new UserBuyCount(DateUtils.getHour(context.window().getStart()), count));
    }
}
