package com.ververica.demo.backend.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.exceptions.RuleNotFoundException;
import com.ververica.demo.backend.model.RulePayload;
import com.ververica.demo.backend.repositories.RuleRepository;
import com.ververica.demo.backend.services.FlinkRulesService;

import java.io.IOException;
import java.util.List;

import org.springframework.web.bind.annotation.*;

/**
 * 规则管理的REST控制器，提供规则的增删改查及推送到Flink服务的API接口。
 */
@RestController
@RequestMapping("/api")
class RuleRestController {

    private final RuleRepository repository; // 规则存储库，用于数据库操作
    private final FlinkRulesService flinkRulesService; // Flink规则服务，用于规则的管理和执行

    // 使用Jackson库的对象映射器，用于JSON数据与Java对象之间的转换
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * 控制器的构造函数，注入所需的服务和存储库。
     */
    RuleRestController(RuleRepository repository, FlinkRulesService flinkRulesService) {
        this.repository = repository;
        this.flinkRulesService = flinkRulesService;
    }

    /**
     * 获取所有规则的列表。
     * @return 规则列表
     */
    @GetMapping("/rules")
    List<Rule> all() {
        return repository.findAll();
    }

    /**
     * 创建新的规则并保存。
     * @param newRule 请求体中包含的新规则对象
     * @return 保存后的规则对象
     * @throws IOException 当JSON处理失败时抛出
     */
    @PostMapping("/rules")
    Rule newRule(@RequestBody Rule newRule) throws IOException {
        Rule savedRule = repository.save(newRule);
        Integer id = savedRule.getId();
        RulePayload payload = mapper.readValue(savedRule.getRulePayload(), RulePayload.class);
        payload.setRuleId(id);
        String payloadJson = mapper.writeValueAsString(payload);
        savedRule.setRulePayload(payloadJson);
        Rule result = repository.save(savedRule);
        flinkRulesService.addRule(result);
        return result;
    }

    /**
     * 将存储库中所有的规则推送到Flink服务。
     */
    @GetMapping("/rules/pushToFlink")
    void pushToFlink() {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            flinkRulesService.addRule(rule);
        }
    }

    /**
     * 根据ID获取单个规则。
     * @param id 规则的ID
     * @return 对应的规则对象
     * @throws RuleNotFoundException 当规则未找到时抛出
     */
    @GetMapping("/rules/{id}")
    Rule one(@PathVariable Integer id) {
        return repository.findById(id).orElseThrow(() -> new RuleNotFoundException(id));
    }

    /**
     * 根据ID删除单个规则。
     * @param id 规则的ID
     * @throws JsonProcessingException 当JSON处理失败时抛出
     */
    @DeleteMapping("/rules/{id}")
    void deleteRule(@PathVariable Integer id) throws JsonProcessingException {
        repository.deleteById(id);
        flinkRulesService.deleteRule(id);
    }

    /**
     * 删除所有规则。
     * @throws JsonProcessingException 当JSON处理失败时抛出
     */
    @DeleteMapping("/rules")
    void deleteAllRules() throws JsonProcessingException {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            repository.deleteById(rule.getId());
            flinkRulesService.deleteRule(rule.getId());
        }
    }
}
