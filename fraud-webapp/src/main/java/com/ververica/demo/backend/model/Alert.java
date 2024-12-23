package com.ververica.demo.backend.model;

import com.ververica.demo.backend.datasource.Transaction;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 警报模型，用于表示触发了某个规则的警报详细信息。
 */
@Data // Lombok注解，自动生成所有属性的getter和setter方法，以及toString、equals和hashCode方法。
@AllArgsConstructor // Lombok注解，自动生成包含所有字段的构造函数。
public class Alert {
    private Integer ruleId; // 规则ID，标识触发警报的规则。
    private String rulePayload; // 规则载荷，包含规则的具体内容。

    Transaction triggeringEvent; // 触发此警报的交易事件。
    BigDecimal triggeringValue; // 触发此警报的数值，可能基于交易金额或其他计算得出。
}
