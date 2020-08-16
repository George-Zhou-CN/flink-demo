package com.fink.demo.model;

import lombok.Data;

/**
 * @Auhtor Jiazhi
 * @Date 2020/8/16 4:25 下午
 **/
@Data
public class CountWithTimestamp {
    /**
     * 分区键
     */
    private String key;

    /**
     * 名字
     */
    private String name;

    /**
     * 计数器
     */
    private Long count;

    /**
     * 最后修改时间
     */
    private Long lastModified;

    public CountWithTimestamp(String key, String name) {
        this.key = key;
        this.name = name;
        this.count = 0L;
    }

    public void increase() {
        if (this.count == null) {
            this.count = 0L;
        }
        this.count++;
    }
}
