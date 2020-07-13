package com.fink.demo.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Objects;

/**
 * Created by Jiazhi on 2020/6/22.
 */
@Data
@NoArgsConstructor
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

    public UserBehavior(UserBehavior userBehavior) {
        this.userId = userBehavior.getUserId();
        this.itemId = userBehavior.getItemId();
        this.categoryId = userBehavior.getCategoryId();
        this.behavior = userBehavior.getBehavior();
        this.ts = userBehavior.getTs();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserBehavior that = (UserBehavior) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(itemId, that.itemId) &&
                Objects.equals(categoryId, that.categoryId) &&
                Objects.equals(behavior, that.behavior) &&
                Objects.equals(ts, that.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, itemId, categoryId, behavior, ts);
    }
}
