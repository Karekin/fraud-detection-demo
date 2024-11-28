package com.ververica.demo.backend.services;

import com.ververica.demo.backend.model.Alert;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka 警报推送服务，用于将警报信息发送到指定的 Kafka 主题。
 */
@Service // 表示这是一个Spring管理的服务类
@Slf4j // Lombok 注解，用于提供日志功能
public class KafkaAlertsPusher implements Consumer<Alert> {

    private final KafkaTemplate<String, Object> kafkaTemplate; // Kafka模板，用于发送消息

    @Value("${kafka.topic.alerts}") // 从配置文件中注入 Kafka 主题的名称
    private String topic;

    /**
     * 构造函数，通过Spring自动注入所需的 KafkaTemplate。
     *
     * @param kafkaTemplateForJson 用于发送 JSON 格式消息的 KafkaTemplate 实例。
     */
    @Autowired
    public KafkaAlertsPusher(KafkaTemplate<String, Object> kafkaTemplateForJson) {
        this.kafkaTemplate = kafkaTemplateForJson;
    }

    /**
     * 实现 Consumer 接口的 accept 方法，用于处理接收到的 Alert 对象并将其发送到 Kafka。
     *
     * @param alert 接收到的警报对象。
     */
    @Override
    public void accept(Alert alert) {
        log.info("Sending alert to Kafka: {}", alert); // 记录日志，显示正在发送的警报信息
        kafkaTemplate.send(topic, alert); // 使用 Kafka 模板发送警报到配置的主题
    }
}
