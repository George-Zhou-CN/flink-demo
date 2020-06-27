package com.fink.demo.functions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/26 8:34 下午
 * @Desc 应用统计
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppStatistics {

    /**
     * 时间
     */
    private String time;

    /**
     * 用户访问人数
     */
    private Long uv;

    /**
     * 页面访问数量
     */
    private Long pv;
}
