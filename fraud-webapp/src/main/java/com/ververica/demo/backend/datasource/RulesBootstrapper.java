package com.ververica.demo.backend.datasource;

import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.repositories.RuleRepository;
import com.ververica.demo.backend.services.FlinkRulesService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 应用启动时执行的组件，用于初始化和注册业务规则。
 * 实现了 ApplicationRunner 接口，使其能在 Spring Boot 应用启动完成后自动执行。
 */
@Component
public class RulesBootstrapper implements ApplicationRunner {

  private final RuleRepository ruleRepository; // 规则存储库，用于持久化规则
  private final FlinkRulesService flinkRulesService; // Flink 规则服务，用于规则的管理和执行

  /**
   * 构造函数，通过依赖注入获取规则存储库和 Flink 规则服务的实例。
   * @param ruleRepository 用于规则存储的仓库
   * @param flinkRulesService Flink的规则服务
   */
  @Autowired
  public RulesBootstrapper(RuleRepository ruleRepository, FlinkRulesService flinkRulesService) {
    this.ruleRepository = ruleRepository;
    this.flinkRulesService = flinkRulesService;
  }

  /**
   * 在应用启动后执行的方法，用于初始化规则数据并注册到相关服务。
   * @param args 应用启动参数
   */
  public void run(ApplicationArguments args) {
    // 定义第一条规则的配置
    String payload1 =
            "{\"ruleId\":\"1\","
                    + "\"aggregateFieldName\":\"paymentAmount\","
                    + "\"aggregatorFunctionType\":\"SUM\","
                    + "\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],"
                    + "\"limit\":\"20000000\","
                    + "\"limitOperatorType\":\"GREATER\","
                    + "\"ruleState\":\"ACTIVE\","
                    + "\"windowMinutes\":\"43200\"}";

    // 创建第一条规则对象
    Rule rule1 = new Rule(payload1);

    // 定义第二条规则的配置
    String payload2 =
            "{\"ruleId\":\"2\","
                    + "\"aggregateFieldName\":\"COUNT_FLINK\","
                    + "\"aggregatorFunctionType\":\"SUM\","
                    + "\"groupingKeyNames\":[\"paymentType\"],"
                    + "\"limit\":\"300\","
                    + "\"limitOperatorType\":\"LESS\","
                    + "\"ruleState\":\"PAUSE\","
                    + "\"windowMinutes\":\"1440\"}";

    // 创建第二条规则对象
    Rule rule2 = new Rule(payload2);

    // 定义第三条规则的配置
    String payload3 =
            "{\"ruleId\":\"3\","
                    + "\"aggregateFieldName\":\"paymentAmount\","
                    + "\"aggregatorFunctionType\":\"SUM\","
                    + "\"groupingKeyNames\":[\"beneficiaryId\"],"
                    + "\"limit\":\"10000000\","
                    + "\"limitOperatorType\":\"GREATER_EQUAL\","
                    + "\"ruleState\":\"ACTIVE\","
                    + "\"windowMinutes\":\"1440\"}";

    // 创建第三条规则对象
    Rule rule3 = new Rule(payload3);

    // 定义第四条规则的配置
    String payload4 =
            "{\"ruleId\":\"4\","
                    + "\"aggregateFieldName\":\"COUNT_WITH_RESET_FLINK\","
                    + "\"aggregatorFunctionType\":\"SUM\","
                    + "\"groupingKeyNames\":[\"paymentType\"],"
                    + "\"limit\":\"100\","
                    + "\"limitOperatorType\":\"GREATER_EQUAL\","
                    + "\"ruleState\":\"ACTIVE\","
                    + "\"windowMinutes\":\"1440\"}";

    // 创建第四条规则对象
    Rule rule4 = new Rule(payload4);

    // 将规则保存到数据库
    ruleRepository.save(rule1);
    ruleRepository.save(rule2);
    ruleRepository.save(rule3);
    ruleRepository.save(rule4);

    // 从数据库中获取所有规则
    List<Rule> rules = ruleRepository.findAll();
    // 将所有规则添加到 Flink 规则服务
    rules.forEach(flinkRulesService::addRule);
  }
}
