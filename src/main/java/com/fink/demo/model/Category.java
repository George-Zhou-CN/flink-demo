package com.fink.demo.model;

import lombok.Data;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 8:09 下午
 * @Desc 类目信息
 **/
@Data
public class Category {
    /**
     * 子类目
     */
    private Long subCategoryId;

    /**
     * 父类目
     */
    private Long parentCategoryId;
}
