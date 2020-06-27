package com.fink.demo.functions;

import com.fink.demo.model.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: jiazhi
 * @Date: 2020/6/24 11:39 下午
 * @Desc:
 */
public class UserBuyCountering implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
