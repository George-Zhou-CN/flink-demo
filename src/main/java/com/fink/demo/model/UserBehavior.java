package com.fink.demo.model;

import com.alibaba.fastjson.annotation.JSONField;
import com.fink.demo.util.DateUtils;
import lombok.Data;

import java.util.Date;

/**
 * Created by Jiazhi on 2020/6/22.
 */
@Data
public class UserBehavior {
    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 商品ID
     */
    private Long itemId;

    /**
     * 商品类目ID
     */
    private Long categoryId;

    /**
     * 行为类型
     */
    private String behavior;

    /**
     * 时间组成
     */
    @JSONField(format = "yyyy-MM-ddTHH:mm:ss")
    private Date ts;

    public String get10Hour() {
        return DateUtils.getHourAnd10Min(this.ts);
    }
}
