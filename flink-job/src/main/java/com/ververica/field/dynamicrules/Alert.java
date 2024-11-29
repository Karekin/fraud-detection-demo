package com.ververica.field.dynamicrules;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 使用Lombok注解自动生成构造函数、getter、setter等方法
@Data // 自动生成getter、setter、toString、equals、hashCode方法
@NoArgsConstructor // 自动生成无参构造函数
@AllArgsConstructor // 自动生成全参构造函数
public class Alert<Event, Value> {

  // 规则ID
  private Integer ruleId;

  // 违反的规则对象
  private Rule violatedRule;

  // 键值，用于标识触发警报的唯一性
  private String key;

  // 触发警报的事件
  private Event triggeringEvent;

  // 触发警报的值
  private Value triggeringValue;
}
