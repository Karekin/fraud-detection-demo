package com.ververica.demo.backend.services;

import com.ververica.demo.backend.datasource.Transaction;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka 交易推送服务，实现了 Consumer 接口，用于接收交易数据并将其发送到 Kafka。
 */
@Service // 标注这是一个Spring服务组件
@Slf4j // 使用Lombok提供的日志功能
public class KafkaTransactionsPusher implements Consumer<Transaction> {

    private final KafkaTemplate<String, Object> kafkaTemplate; // Kafka模板，用于发送数据到Kafka
    private Transaction lastTransaction; // 用于存储最后一次处理的交易数据

    @Value("${kafka.topic.transactions}")
    private String topic; // 从配置文件中读取的 Kafka 主题名称

    /**
     * 构造函数，自动注入 KafkaTemplate。
     * @param kafkaTemplateForJson 用于发送 JSON 格式消息的 KafkaTemplate 实例。
     */
    @Autowired
    public KafkaTransactionsPusher(KafkaTemplate<String, Object> kafkaTemplateForJson) {
        this.kafkaTemplate = kafkaTemplateForJson;
    }

    /**
     * 实现 Consumer 接口的 accept 方法，用于处理交易数据。
     * @param transaction 接收到的交易数据
     */
    @Override
    public void accept(Transaction transaction) {
        lastTransaction = transaction; // 更新最后一次处理的交易数据
        log.debug("Pushing transaction to Kafka: {}", transaction); // 记录日志
        kafkaTemplate.send(topic, transaction); // 将交易数据发送到配置的 Kafka 主题
    }

    /**
     * 获取最后一次处理的交易数据。
     * @return 最后一次处理的交易对象
     */
    public Transaction getLastTransaction() {
        return lastTransaction;
    }
}
