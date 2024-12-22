package com.ververica.field.dynamicrules;

import java.math.BigDecimal;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.api.common.time.Time;

/**
 * 规则表示类，包含规则的各类属性及逻辑
 */
@EqualsAndHashCode // 自动生成equals()和hashCode()方法
@ToString // 自动生成toString()方法
@Data // 自动生成getter、setter、toString、equals、hashCode方法
public class Rule {

    // 规则ID
    private Integer ruleId;

    // 规则状态（例如：激活、暂停、删除等）
    private RuleState ruleState;

    // 分组键名称（用于聚合）
    private List<String> groupingKeyNames;

    // 唯一标识符列表（用于唯一约束）
    private List<String> unique;

    // 聚合字段名称
    private String aggregateFieldName;

    // 聚合函数类型（如：SUM、AVG等）
    private AggregatorFunctionType aggregatorFunctionType;

    // 限制操作符类型（如：>、<=等）
    private LimitOperatorType limitOperatorType;

    // 限制值（规则比较的阈值）
    private BigDecimal limit;

    // 窗口时间（单位：分钟）
    private Integer windowMinutes;

    // 控制类型（如：清除状态、删除规则等）
    private ControlType controlType;

    /**
     * 获取窗口时间的毫秒数
     *
     * @return 窗口时间对应的毫秒数
     */
    public Long getWindowMillis() {
        // 将分钟转换为毫秒
        return Time.minutes(this.windowMinutes).toMilliseconds();
    }

    /**
     * 根据提供的值与规则的限制进行比较，返回规则是否满足
     *
     * @param comparisonValue 需要与规则的限制值进行比较的值
     * @return 如果规则满足限制，则返回true，否则返回false
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                // 等于
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                // 不等于
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                // 大于
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                // 小于
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                // 小于或等于
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                // 大于或等于
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("未知的限制操作符类型: " + limitOperatorType);
        }
    }

    /**
     * 根据给定的时间戳计算窗口的起始时间
     *
     * @param timestamp 当前时间戳
     * @return 规则窗口的起始时间
     */
    public long getWindowStartFor(Long timestamp) {
        Long ruleWindowMillis = getWindowMillis();
        // 计算窗口开始时间
        return (timestamp - ruleWindowMillis);
    }

    /**
     * 聚合函数类型
     */
    public enum AggregatorFunctionType {
        SUM, // 求和
        AVG, // 平均值
        MIN, // 最小值
        MAX  // 最大值
    }

    /**
     * 限制操作符类型，用于与值进行比较的操作符
     */
    public enum LimitOperatorType {
        EQUAL("="), // 等于
        NOT_EQUAL("!="), // 不等于
        GREATER_EQUAL(">="), // 大于或等于
        LESS_EQUAL("<="), // 小于或等于
        GREATER(">"), // 大于
        LESS("<"); // 小于

        // 操作符字符串
        final String operator;

        // 构造函数
        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        /**
         * 根据字符串获取相应的操作符类型
         *
         * @param text 操作符字符串
         * @return 对应的LimitOperatorType
         */
        public static LimitOperatorType fromString(String text) {
            // 遍历所有枚举值，匹配字符串
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    /**
     * 规则状态，表示规则当前的执行状态
     */
    public enum RuleState {
        ACTIVE, // 激活
        PAUSE, // 暂停
        DELETE, // 删除
        CONTROL // 控制
    }

    /**
     * 控制类型，表示在规则执行过程中可能进行的控制操作
     */
    public enum ControlType {
        CLEAR_STATE_ALL, // 清除所有状态
        CLEAR_STATE_ALL_STOP, // 清除所有状态并停止
        DELETE_RULES_ALL, // 删除所有规则
        EXPORT_RULES_CURRENT // 导出当前规则
    }
}
