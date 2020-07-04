package com.fink.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 10:32 下午
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopCategory {

    private String categoryName;

    private Long buyCount;

    public TopCategory increase() {
        if (this.buyCount == null) {
            this.buyCount = 1L;
            return this;
        }
        this.buyCount++;
        return this;
    }

    public Boolean isEmpty() {
        return categoryName == null && buyCount == null;
    }

    public TopCategory merge(TopCategory otherTopCategory) {
        return new TopCategory(this.categoryName,
                otherTopCategory.getBuyCount() + otherTopCategory.getBuyCount());
    }
}
