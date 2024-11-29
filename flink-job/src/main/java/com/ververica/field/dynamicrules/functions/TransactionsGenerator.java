package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.Transaction.PaymentType;
import com.ververica.field.sources.BaseGenerator;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

/**
 * TransactionsGenerator 类用于生成模拟交易数据。
 * 它继承自 BaseGenerator<Transaction>，负责随机生成交易事件，并返回一个 Transaction 对象。
 */
public class TransactionsGenerator extends BaseGenerator<Transaction> {

    // 最大收款人 ID 范围
    private static long MAX_PAYEE_ID = 100000;

    // 最大受益人 ID 范围
    private static long MAX_BENEFICIARY_ID = 100000;

    // 支付金额的最小值和最大值
    private static double MIN_PAYMENT_AMOUNT = 5d;
    private static double MAX_PAYMENT_AMOUNT = 20d;

    /**
     * 构造函数，接收最大记录数每秒限制。
     *
     * @param maxRecordsPerSecond 每秒生成的最大记录数
     */
    public TransactionsGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    /**
     * 随机生成一个交易事件（Transaction），返回一个包含随机数据的 Transaction 对象。
     *
     * @param rnd 随机数生成器，用于生成随机数据
     * @param id  当前子任务的 ID，用于区分不同子任务生成的数据
     * @return 生成的交易对象
     */
    @Override
    public Transaction randomEvent(SplittableRandom rnd, long id) {
        // 随机生成交易 ID
        long transactionId = rnd.nextLong(Long.MAX_VALUE);

        // 随机生成收款人 ID，范围从 0 到 MAX_PAYEE_ID
        long payeeId = rnd.nextLong(MAX_PAYEE_ID);

        // 随机生成受益人 ID，范围从 0 到 MAX_BENEFICIARY_ID
        long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);

        // 随机生成支付金额，范围从 MIN_PAYMENT_AMOUNT 到 MAX_PAYMENT_AMOUNT
        double paymentAmountDouble =
                ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
        // 保留两位小数，模拟实际的支付金额
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
        // 将支付金额转换为 BigDecimal 类型，以便精确计算
        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);

        // 构建 Transaction 对象
        Transaction transaction =
                Transaction.builder()
                        .transactionId(transactionId)       // 设置交易 ID
                        .payeeId(payeeId)                   // 设置收款人 ID
                        .beneficiaryId(beneficiaryId)       // 设置受益人 ID
                        .paymentAmount(paymentAmount)       // 设置支付金额
                        .paymentType(paymentType(transactionId)) // 设置支付类型，根据交易 ID 决定
                        .eventTime(System.currentTimeMillis()) // 设置事件发生时间
                        .ingestionTimestamp(System.currentTimeMillis()) // 设置数据摄取时间
                        .build();                          // 创建并返回 Transaction 对象

        return transaction;
    }

    /**
     * 根据交易 ID 生成支付类型。支付类型分为信用卡（CRD）和现金（CSH）。
     *
     * @param id 交易 ID
     * @return 生成的支付类型（PaymentType.CRD 或 PaymentType.CSH）
     */
    private PaymentType paymentType(long id) {
        // 使用交易 ID 进行简单的取模操作来决定支付类型
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return PaymentType.CRD;  // 如果 id 是偶数，则支付类型为信用卡
            case 1:
                return PaymentType.CSH;  // 如果 id 是奇数，则支付类型为现金
            default:
                throw new IllegalStateException("Invalid payment type"); // 如果出现其他情况，抛出异常
        }
    }
}
