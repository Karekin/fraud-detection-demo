package com.ververica.field.functions;

import com.ververica.field.model.Alert;
import com.ververica.field.model.Rule;
import com.ververica.field.model.Transaction;
import org.apache.flink.cep.context.RuleAwareContext;
import org.apache.flink.cep.event.RuleUpdated;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.*;

/**
 * 对匹配到的数据取平均值，并输出阈值
 *
 * @author shirukai
 */
public class MyPatternProcessFunction extends PatternProcessFunction<Transaction, Alert> {
    private static final ConfigOption<Double> TEMP_THRESHOLD = ConfigOptions.key("temp_threshold")
            .doubleType().defaultValue(100.0);

    private static final ConfigOption<Double> RPM_THRESHOLD = ConfigOptions.key("rpm_threshold")
            .doubleType().defaultValue(6000.0);

    private double tempThreshold;
    private double rpmThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        tempThreshold = parameters.get(TEMP_THRESHOLD);
        rpmThreshold = parameters.get(RPM_THRESHOLD);
    }

    @Override
    public void processMatch(
            Map<String, List<Transaction>> match,
            Context ctx,
            Collector<Alert> out) throws Exception {
        // 从匹配的事件中获取 "start" 事件列表
        List<Transaction> events = match.get("start");
        if (events == null || events.isEmpty()) {
            return;
        }

        if (ctx instanceof RuleAwareContext) {
            RuleUpdated currentRule = ((RuleAwareContext) ctx).getCurrentRule();
            System.out.println("Current Rule: " + currentRule.getId());

            // 获取首个事件
            Transaction firstEvent = events.get(0);

            // 创建 Rule 实例
            Rule violatedRule = new Rule();
            violatedRule.setRuleId(currentRule.getVersion());
            violatedRule.setRuleState(Rule.RuleState.ACTIVE);
            violatedRule.setGroupingKeyNames(Collections.singletonList("beneficiaryId"));
            violatedRule.setAggregateFieldName("paymentAmount");
            violatedRule.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);
            violatedRule.setLimitOperatorType(Rule.LimitOperatorType.GREATER_EQUAL);
            violatedRule.setLimit(BigDecimal.valueOf(10_000_000));
            violatedRule.setWindowMinutes(1440);

            // 构造 Alert 实例
            Alert<Transaction, Integer> alert = new Alert<>(
                    violatedRule.getRuleId(),                           // ruleId
                    violatedRule,                // violatedRule
                    UUID.randomUUID().toString(),           // key
                    firstEvent,                  // triggeringEvent
                    10185131                     // triggeringValue
            );

            // 输出 Alert
            out.collect(alert);

            // 根据当前规则进行处理
        } else {
            throw new IllegalStateException("Context does not support Rule awareness.");
        }
    }



//    @Override
//    public void processMatch(
//            Map<String, List<Transaction>> match,
//            Context ctx,
//            Collector<Transaction> out) throws Exception {
//        List<Transaction> events = match.get("start");
//        if (events == null || events.isEmpty()) {
//            return;
//        }
//
//        // 1. 计算均值
////        double rpmAvg = events.stream().mapToDouble(Transaction::getRpm).average().orElse(0.0);
////        double tempAvg = events.stream().mapToDouble(Transaction::getTemp).average().orElse(0.0);
//
//        // 2. 构造输出事件
//        Transaction firstEvent = events.get(0);
////        Transaction resultEvent = new Transaction(
////                firstEvent.transactionId,
////                firstEvent.g,
////                tempAvg,
////                (long) rpmAvg,
////                firstEvent.getDetectionTime()
////        );
//
//        // 3. 输出结果
//        out.collect(firstEvent);
//    }
}