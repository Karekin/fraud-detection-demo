import {RefObject} from "react"; // 从 React 库导入 RefObject 类型，用于引用 React 元素。

/**
 * 规则接口定义，用于描述一个业务规则的主要属性及其关联的 HTML 元素引用。
 */
export interface Rule {
    id: number; // 规则的唯一标识符。
    rulePayload: string; // 规则的详细数据载荷，通常为 JSON 字符串格式。
    ref: RefObject<HTMLDivElement>; // React 引用对象，用于直接操作 DOM 元素，这里指向一个 HTMLDivElement。
}

/**
 * 规则载荷接口定义，描述具体的规则逻辑和参数。
 */
export interface RulePayload {
    aggregateFieldName: string; // 聚合字段名，指定哪个字段将用于聚合计算。
    aggregatorFunctionType: string; // 聚合函数类型，如 "SUM", "AVG", "MIN", "MAX" 等。
    groupingKeyNames: string[]; // 分组键名称列表，指定如何将数据分组进行聚合。
    limit: number; // 阈值，与 limitOperatorType 一起定义触发规则的条件。
    limitOperatorType: string; // 限制操作类型，定义如何比较聚合结果与阈值，例如 "GREATER", "LESS", "EQUAL" 等。
    windowMinutes: number; // 时间窗口（分钟），指定聚合计算的时间范围。
    ruleState: string; // 规则状态，如 "ACTIVE", "INACTIVE", "DELETED" 等，用于控制规则是否应用。
}
