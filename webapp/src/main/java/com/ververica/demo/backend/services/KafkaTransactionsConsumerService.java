package com.ververica.demo.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka 交易数据消费服务，实现了消费者位置感知，可以根据需要调整消费者读取的位置。
 */
@Service
@Slf4j
public class KafkaTransactionsConsumerService implements ConsumerSeekAware {

    private final SimpMessagingTemplate simpTemplate; // WebSocket消息模板
    private final ObjectMapper mapper = new ObjectMapper(); // JSON对象映射器

    @Value("${web-socket.topic.transactions}")
    private String transactionsWebSocketTopic; // WebSocket主题，用于发送交易数据

    /**
     * 构造函数，注入WebSocket消息模板。
     * @param simpTemplate 用于WebSocket消息发送的模板
     */
    @Autowired
    public KafkaTransactionsConsumerService(SimpMessagingTemplate simpTemplate) {
        this.simpTemplate = simpTemplate;
    }

    /**
     * 监听Kafka主题，消费交易数据，并通过WebSocket推送到客户端。
     * @param message 从Kafka接收的交易数据
     */
    @KafkaListener(
            id = "${kafka.listeners.transactions.id}",
            topics = "${kafka.topic.transactions}",
            groupId = "transactions")
    public void consumeTransactions(@Payload String message) {
        log.debug("Received transaction: {}", message);
        simpTemplate.convertAndSend(transactionsWebSocketTopic, message);
    }

    /**
     * 注册一个回调函数，用于在消费者分配分区后调整消费位置。
     */
    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        // 这里通常不做实现，除非你需要在回调注册时执行操作
    }

    /**
     * 当分区被分配给消费者时调用，可以用来调整消费者的初始偏移量。
     * @param assignments 分区及其对应的偏移量的映射
     * @param callback 用于执行寻址操作的回调
     */
    @Override
    public void onPartitionsAssigned(
            Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // 对每个分配的分区进行操作，如移动到分区末尾开始消费
        assignments.forEach((topicPartition, offset) -> callback.seekToEnd(topicPartition.topic(), topicPartition.partition()));
    }

    /**
     * 当消费者空闲时调用，可以在这里执行相关的检查或重新定位。
     * @param assignments 分区及其对应的偏移量的映射
     * @param callback 用于执行寻址操作的回调
     */
    @Override
    public void onIdleContainer(
            Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // 空闲时不进行任何操作，但可以用于实现如心跳检测等功能
    }
}
