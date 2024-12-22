package com.ververica.field.dynamicrules;

import static com.ververica.field.config.Parameters.KAFKA_HOST;
import static com.ververica.field.config.Parameters.KAFKA_PORT;
import static com.ververica.field.config.Parameters.OFFSET;

import com.ververica.field.config.Config;

import java.util.Properties;

// Kafka相关的工具类，用于初始化消费者和生产者的配置属性
public class KafkaUtils {

    // 初始化消费者的配置属性
    public static Properties initConsumerProperties(Config config) {
        // 初始化基本的Kafka属性
        Properties kafkaProps = initProperties(config);
        // 获取配置中的偏移量（Offset）设置
        String offset = config.get(OFFSET);
        // 设置消费者的偏移量策略
        kafkaProps.setProperty("auto.offset.reset", offset);
        // 返回完整的消费者配置属性
        return kafkaProps;
    }

    // 初始化生产者的配置属性
    public static Properties initProducerProperties(Config params) {
        // 初始化并返回生产者的Kafka属性配置
        return initProperties(params);
    }

    // 初始化Kafka的基本配置属性
    private static Properties initProperties(Config config) {
        // 创建一个Properties对象，用于存储Kafka的配置
        Properties kafkaProps = new Properties();
        // 获取Kafka主机地址
        String kafkaHost = config.get(KAFKA_HOST);
        // 获取Kafka端口号
        int kafkaPort = config.get(KAFKA_PORT);
        // 格式化并设置Kafka服务器地址（host:port）
        String servers = String.format("%s:%s", kafkaHost, kafkaPort);
        // 设置bootstrap.servers属性，指定Kafka集群的地址
        kafkaProps.setProperty("bootstrap.servers", servers);
        // 返回配置好的Kafka属性
        return kafkaProps;
    }
}
