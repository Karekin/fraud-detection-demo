import {Transaction} from "./Transaction"; // 导入 Transaction 接口，表示触发警报的交易数据结构。
import {RefObject} from "react"; // 从 React 库导入 RefObject 类型，用于引用 React 元素。
import {RulePayload} from "./Rule"; // 导入 RulePayload 接口，表示规则的详细数据结构。

/**
 * 警报接口定义，用于描述触发的警报及其相关信息。
 */
export interface Alert {
    alertId: string; // 警报的唯一标识符，用于区分不同的警报。
    ruleId: number; // 触发警报的规则的唯一标识符。
    violatedRule: RulePayload; // 被违反的规则的详细信息，使用 RulePayload 接口描述规则内容。
    triggeringValue: number; // 触发警报的值，表示实际的交易或计算值超过了规则限制。
    triggeringEvent: Transaction; // 触发警报的交易事件，包含交易的详细信息。
    ref: RefObject<HTMLDivElement>; // React 引用对象，指向与警报相关的 HTMLDivElement，便于操作相关 DOM 元素。
}
