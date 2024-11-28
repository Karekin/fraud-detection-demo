package com.ververica.demo.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.model.RulePayload;
import com.ververica.demo.backend.repositories.RuleRepository;

import java.io.IOException;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka 消费者服务，用于监听 Kafka 主题的消息，并执行相关的业务逻辑。
 */
@Service
@Slf4j
public class KafkaConsumerService {

    private final SimpMessagingTemplate simpTemplate; // WebSocket消息模板
    private final RuleRepository ruleRepository; // 规则存储库
    private final ObjectMapper mapper = new ObjectMapper(); // 用于JSON序列化和反序列化

    @Value("${web-socket.topic.alerts}")
    private String alertsWebSocketTopic; // WebSocket主题，用于发送警报信息

    @Value("${web-socket.topic.latency}")
    private String latencyWebSocketTopic; // WebSocket主题，用于发送延迟信息

    /**
     * 构造函数，通过依赖注入获取所需的组件。
     * @param simpTemplate 用于发送WebSocket消息的模板
     * @param ruleRepository 用于操作规则数据的存储库
     */
    @Autowired
    public KafkaConsumerService(SimpMessagingTemplate simpTemplate, RuleRepository ruleRepository) {
        this.simpTemplate = simpTemplate;
        this.ruleRepository = ruleRepository;
    }

    /**
     * 监听警报相关的 Kafka 主题，接收消息并通过 WebSocket 发送。
     * @param message 从 Kafka 接收到的警报消息
     */
    @KafkaListener(topics = "${kafka.topic.alerts}", groupId = "alerts")
    public void templateAlerts(@Payload String message) {
        log.debug("Received alert message: {}", message);
        simpTemplate.convertAndSend(alertsWebSocketTopic, message);
    }

    /**
     * 监听延迟相关的 Kafka 主题，接收消息并通过 WebSocket 发送。
     * @param message 从 Kafka 接收到的延迟消息
     */
    @KafkaListener(topics = "${kafka.topic.latency}", groupId = "latency")
    public void templateLatency(@Payload String message) {
        log.debug("Received latency message: {}", message);
        simpTemplate.convertAndSend(latencyWebSocketTopic, message);
    }

    /**
     * 监听当前规则相关的 Kafka 主题，接收消息并更新或保存新的规则到数据库。
     * @param message 从 Kafka 接收到的当前规则消息
     * @throws IOException 当消息反序列化失败时抛出
     */
    @KafkaListener(topics = "${kafka.topic.current-rules}", groupId = "current-rules")
    public void templateCurrentFlinkRules(@Payload String message) throws IOException {
        log.info("Received current rule message: {}", message);
        RulePayload payload = mapper.readValue(message, RulePayload.class);
        Integer payloadId = payload.getRuleId();
        Optional<Rule> existingRule = ruleRepository.findById(payloadId);
        if (!existingRule.isPresent()) {
            ruleRepository.save(new Rule(payloadId, mapper.writeValueAsString(payload)));
        }
    }
}
