package com.ververica.demo.backend.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.model.RulePayload;
import com.ververica.demo.backend.model.RulePayload.ControlType;
import com.ververica.demo.backend.model.RulePayload.RuleState;
import com.ververica.demo.backend.services.FlinkRulesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Flink控制器，用于管理与Flink相关的操作，例如同步规则和清除状态。
 */
@RestController
@RequestMapping("/api")
public class FlinkController {

    private final FlinkRulesService flinkRulesService; // Flink规则服务

    /**
     * 构造函数，注入Flink规则服务。
     * @param flinkRulesService Flink规则服务，用于向Flink作业发送控制命令。
     */
    FlinkController(FlinkRulesService flinkRulesService) {
        this.flinkRulesService = flinkRulesService;
    }

    private final ObjectMapper mapper = new ObjectMapper(); // JSON对象映射器，用于对象与JSON的转换。

    /**
     * 同步规则到Flink作业。发送当前导出规则的控制命令。
     * @throws JsonProcessingException 如果JSON序列化失败。
     */
    @GetMapping("/syncRules")
    void syncRules() throws JsonProcessingException {
        Rule command = createControllCommand(ControlType.EXPORT_RULES_CURRENT);
        flinkRulesService.addRule(command);
    }

    /**
     * 清除Flink作业的所有状态。发送清除状态的控制命令。
     * @throws JsonProcessingException 如果JSON序列化失败。
     */
    @GetMapping("/clearState")
    void clearState() throws JsonProcessingException {
        Rule command = createControllCommand(ControlType.CLEAR_STATE_ALL);
        flinkRulesService.addRule(command);
    }

    /**
     * 创建一个控制命令的规则对象。
     * @param controlType 控制类型，指定要执行的控制操作。
     * @return 构造的控制命令规则对象。
     * @throws JsonProcessingException 如果JSON序列化失败。
     */
    private Rule createControllCommand(ControlType controlType) throws JsonProcessingException {
        RulePayload payload = new RulePayload(); // 创建规则负载对象
        payload.setRuleState(RuleState.CONTROL); // 设置规则状态为控制
        payload.setControlType(controlType); // 设置控制类型
        Rule rule = new Rule(); // 创建规则对象
        rule.setRulePayload(mapper.writeValueAsString(payload)); // 序列化规则负载为JSON字符串并设置到规则对象
        return rule; // 返回构造的规则对象
    }
}
