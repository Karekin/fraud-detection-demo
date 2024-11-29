package com.ververica.field.dynamicrules;

import com.ververica.field.dynamicrules.accumulators.AverageAccumulator;
import com.ververica.field.dynamicrules.accumulators.BigDecimalCounter;
import com.ververica.field.dynamicrules.accumulators.BigDecimalMaximum;
import com.ververica.field.dynamicrules.accumulators.BigDecimalMinimum;

import java.math.BigDecimal;

import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * 提供规则相关的辅助方法集合。
 * 包含根据规则的聚合函数类型选择合适的累加器。
 */
public class RuleHelper {

    /**
     * 根据规则的聚合函数类型选择并返回相应的累加器。
     *
     * @param rule 规则对象，根据规则的聚合函数类型来选择累加器
     * @return 返回与规则的聚合函数类型相匹配的累加器
     * @throws RuntimeException 如果遇到不支持的聚合函数类型，则抛出异常
     */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        // 根据规则的聚合函数类型返回相应的累加器
        switch (rule.getAggregatorFunctionType()) {
            case SUM:
                // 如果聚合函数类型为SUM（求和），返回BigDecimalCounter累加器
                return new BigDecimalCounter();
            case AVG:
                // 如果聚合函数类型为AVG（平均值），返回AverageAccumulator累加器
                return new AverageAccumulator();
            case MAX:
                // 如果聚合函数类型为MAX（最大值），返回BigDecimalMaximum累加器
                return new BigDecimalMaximum();
            case MIN:
                // 如果聚合函数类型为MIN（最小值），返回BigDecimalMinimum累加器
                return new BigDecimalMinimum();
            default:
                // 如果聚合函数类型不支持，则抛出运行时异常
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
        }
    }
}
