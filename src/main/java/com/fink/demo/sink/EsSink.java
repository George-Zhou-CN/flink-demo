package com.fink.demo.sink;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/5 12:43 下午
 **/
public class EsSink {
    private final static String HOST = "localhost";
    private final static Integer PORT = 9200;
    private final static String SCHEMA = "http";

    public static <T> ElasticsearchSink<T> buildSink(ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        // 写入ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(HOST, PORT, SCHEMA));
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        // 关闭ES写入缓存
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder.build();
    }
}
