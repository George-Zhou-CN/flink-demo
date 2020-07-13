package com.fink.demo.functions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/26 4:25 下午
 * Desc 每10分钟UV
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UvPer10Min {

    /**
     * 时间，（格式：HH:mm）
     */
    private Date time;

    /**
     *
     */
    private String key;

    /**
     * 用户访问人数
     */
    private Long uv;
}
