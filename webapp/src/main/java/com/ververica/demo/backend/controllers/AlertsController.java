package com.ververica.demo.backend.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.datasource.Transaction;
import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.exceptions.RuleNotFoundException;
import com.ververica.demo.backend.model.Alert;
import com.ververica.demo.backend.repositories.RuleRepository;
import com.ververica.demo.backend.services.KafkaTransactionsPusher;

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 警报控制器，负责处理与警报生成和通知相关的API请求。
 */
@RestController
@RequestMapping("/api")
public class AlertsController {

    private final RuleRepository repository; // 规则存储库
    private final KafkaTransactionsPusher transactionsPusher; // Kafka交易推送服务
    private SimpMessagingTemplate simpSender; // WebSocket消息发送模板

    @Value("${web-socket.topic.alerts}")
    private String alertsWebSocketTopic; // WebSocket主题配置，用于发送警报消息

    /**
     * 构造函数，通过Spring依赖注入获取所需的服务和组件。
     * @param repository 规则存储库
     * @param transactionsPusher Kafka交易推送服务
     * @param simpSender WebSocket消息发送模板
     */
    @Autowired
    public AlertsController(
            RuleRepository repository,
            KafkaTransactionsPusher transactionsPusher,
            SimpMessagingTemplate simpSender) {
        this.repository = repository;
        this.transactionsPusher = transactionsPusher;
        this.simpSender = simpSender;
    }

    private final ObjectMapper mapper = new ObjectMapper(); // JSON映射器，用于对象序列化

    /**
     * 根据规则ID生成一个模拟警报，并通过WebSocket发送警报数据。
     * @param id 规则ID
     * @return 生成的警报对象
     * @throws JsonProcessingException 如果序列化警报对象失败
     */
    @GetMapping("/rules/{id}/alert")
    Alert mockAlert(@PathVariable Integer id) throws JsonProcessingException {
        Rule rule = repository.findById(id).orElseThrow(() -> new RuleNotFoundException(id)); // 查询规则，如果不存在抛出异常
        Transaction triggeringEvent = transactionsPusher.getLastTransaction(); // 获取最后一条交易数据作为触发事件
        String violatedRule = rule.getRulePayload(); // 获取被触发的规则详情
        BigDecimal triggeringValue = triggeringEvent.getPaymentAmount().multiply(new BigDecimal(10)); // 计算触发值，此处为示例逻辑

        Alert alert = new Alert(rule.getId(), violatedRule, triggeringEvent, triggeringValue); // 创建警报对象

        String result = mapper.writeValueAsString(alert); // 将警报对象序列化为JSON字符串

        simpSender.convertAndSend(alertsWebSocketTopic, result); // 通过WebSocket发送警报

        return alert; // 返回生成的警报对象
    }
}
