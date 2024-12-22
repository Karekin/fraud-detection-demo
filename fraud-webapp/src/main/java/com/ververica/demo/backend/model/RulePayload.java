package com.ververica.demo.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 表示规则的数据模型，用于处理规则的各种操作。
 */
@EqualsAndHashCode // Lombok注解，为类提供equals和hashCode方法。
@ToString // Lombok注解，为类提供toString方法。
@Data // Lombok注解，为属性自动生成getter/setter。
@JsonIgnoreProperties(ignoreUnknown = true) // Jackson注解，忽略JSON中未知属性，防止解析错误。
public class RulePayload {

    private Integer ruleId; // 规则的ID
    private RuleState ruleState; // 规则的状态，如激活、暂停等
    private List<String> groupingKeyNames; // 聚合键名列表，用于分组聚合操作
    private List<String> unique; // 唯一性字段列表，用于数据唯一性校验
    private String aggregateFieldName; // 聚合字段名
    private AggregatorFunctionType aggregatorFunctionType; // 聚合函数类型，如SUM、AVG等
    private LimitOperatorType limitOperatorType; // 限制操作类型，如大于、小于等
    private BigDecimal limit; // 规则设定的阈值
    private Integer windowMinutes; // 规则的时间窗口，以分钟为单位
    private ControlType controlType; // 控制类型，定义规则的控制操作

    /**
     * 根据规则的限制操作类型应用比较逻辑。
     * @param comparisonValue 需要与规则限制比较的值
     * @return 比较结果，符合规则时返回true，否则返回false。
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }

    // 定义聚合函数类型枚举
    public enum AggregatorFunctionType {
        SUM, // 求和
        AVG, // 平均值
        MIN, // 最小值
        MAX  // 最大值
    }

    // 定义限制操作类型枚举
    public enum LimitOperatorType {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        GREATER(">"),
        LESS("<");

        final String operator; // 操作符表示

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        /**
         * 从字符串中解析限制操作类型。
         * @param text 表示操作类型的字符串
         * @return 对应的LimitOperatorType枚举值，如果找不到匹配项则返回null。
         */
        public static LimitOperatorType fromString(String text) {
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    // 定义规则状态枚举
    public enum RuleState {
        ACTIVE,   // 激活
        PAUSE,    // 暂停
        DELETE,   // 删除
        CONTROL   // 控制
    }

    // 定义控制类型枚举
    public enum ControlType {
        CLEAR_STATE_ALL,             // 清除所有状态
        CLEAR_STATE_ALL_STOP,        // 清除所有状态并停止
        DELETE_RULES_ALL,            // 删除所有规则
        EXPORT_RULES_CURRENT         // 导出当前规则
    }
}
