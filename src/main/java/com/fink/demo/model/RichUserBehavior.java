package com.fink.demo.model;

import lombok.Data;
import lombok.Getter;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 10:14 下午
 **/
@Data
@Getter
public class RichUserBehavior extends UserBehavior {

    /**
     * 父类目ID
     */
    private Long parentCategoryId;

    /**
     * 父类目名称
     */
    private String parentCategoryName;

    public RichUserBehavior(UserBehavior userBehavior, Category category) {
        super(userBehavior);

        this.parentCategoryId = category.getParentCategoryId();
        if (this.parentCategoryId == 1L) {
            this.parentCategoryName = "服饰鞋包";
        } else if (this.parentCategoryId == 2L) {
            this.parentCategoryName = "家装家饰";
        } else if (this.parentCategoryId == 3L) {
            this.parentCategoryName = "家电";
        } else if (this.parentCategoryId == 4L) {
            this.parentCategoryName = "美妆";
        } else if (this.parentCategoryId == 5L) {
            this.parentCategoryName = "母婴";
        } else if (this.parentCategoryId == 6L) {
            this.parentCategoryName = "3C数码";
        } else if (this.parentCategoryId == 7L) {
            this.parentCategoryName = "运动户外";
        } else if (this.parentCategoryId == 8L) {
            this.parentCategoryName = "食品";
        } else {
            this.parentCategoryName = "其他";
        }
    }
}
