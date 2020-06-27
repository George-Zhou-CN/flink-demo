package com.fink.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: jiazhi
 * @Date: 2020/6/24 10:56 下午
 * @Desc:
 */
@Data
@AllArgsConstructor
public class UserBuyCount {

    /**
     * 每小时
     */
    private Integer hour;

    /**
     * 成交量
     */
    private Long count;
}
