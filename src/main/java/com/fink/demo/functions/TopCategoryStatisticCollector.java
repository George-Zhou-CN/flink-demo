package com.fink.demo.functions;

import com.fink.demo.model.TopCategory;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 10:50 下午
 **/
public class TopCategoryStatisticCollector extends ProcessWindowFunction<TopCategory, TopCategory, Long, TimeWindow> {

    @Override
    public void process(Long aLong, Context context, Iterable<TopCategory> elements, Collector<TopCategory> out) throws Exception {
        out.collect(elements.iterator().next());
    }
}
