// import { RefObject } from "react";

// 示例消息格式注释:
// MSG
// beneficiaryId: 42694
// eventTime: 1565965071385
// payeeId: 20908
// paymentAmount: 13.54
// paymentType: "CRD"
// transactionId: 5954524216210268000

/**
 * 交易数据接口定义。
 * 定义交易对象的结构，用于在应用中传递和存储交易信息。
 */
export interface Transaction {
    beneficiaryId: number; // 受益人ID，标识收款方的唯一标识符。
    eventTime: number; // 事件时间，使用 Unix 时间戳（毫秒）表示交易发生的具体时间。
    payeeId: number; // 付款人ID，标识付款方的唯一标识符。
    paymentAmount: number; // 交易金额，表示交易中转移的货币数量。
    paymentType: string; // 支付类型，如 "CRD" 表示信用卡支付等。
    transactionId: number; // 交易ID，交易的唯一标识符，用于追踪和记录每一笔交易。
}
