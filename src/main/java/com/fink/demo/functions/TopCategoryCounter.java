package com.fink.demo.functions;

import com.fink.demo.model.RichUserBehavior;
import com.fink.demo.model.TopCategory;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 10:49 下午
 **/
public class TopCategoryCounter implements AggregateFunction<RichUserBehavior, TopCategory, TopCategory> {

    @Override
    public TopCategory createAccumulator() {
        return new TopCategory();
    }

    @Override
    public TopCategory add(RichUserBehavior value, TopCategory accumulator) {
        if (accumulator.isEmpty()) {
            accumulator.setCategoryId(value.getParentCategoryId());
            accumulator.setCategoryName(value.getParentCategoryName());
            accumulator.setBuyCount(1L);
            return accumulator;
        }

        return accumulator.increase();
    }

    @Override
    public TopCategory getResult(TopCategory accumulator) {
        return accumulator;
    }

    @Override
    public TopCategory merge(TopCategory a, TopCategory b) {
        return a.merge(b);
    }
}
