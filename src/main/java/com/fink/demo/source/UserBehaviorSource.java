package com.fink.demo.source;

import com.fink.demo.model.UserBehavior;
import com.fink.demo.serialization.UserBehaviorDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Auhtor Jiazhi
 * @Date 2020/6/26 2:23 下午
 * @Desc 用户行为数据源
 **/
public class UserBehaviorSource {
    private final static String INPUT_TOPIC = "user_behavior";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String ZOOKEEPER_CONNECT = "localhost:2181";
    private final static String GROUP_ID_CONFIG = "user-behavior";

    public static FlinkKafkaConsumer<UserBehavior> getSource() {
        // 接收数据源
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_CONNECT);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);

        return new FlinkKafkaConsumer<>(INPUT_TOPIC, new UserBehaviorDeserializationSchema(), kafkaProps);
    }

}
