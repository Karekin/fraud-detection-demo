package com.ververica.demo.backend.datasource;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

/**
 * 用于演示目的的交易数据生成器。
 * 扩展了TransactionsGenerator类，增加了特定的支付条件模拟。
 */
@Slf4j
public class DemoTransactionsGenerator extends TransactionsGenerator {

    // 上一次触发收款人ID的时间戳
    private long lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
    // 上一次触发支付人ID的时间戳
    private long lastBeneficiaryIdTriggered = System.currentTimeMillis();
    // 触发大额交易的收款人限额
    private final BigDecimal beneficiaryLimit = new BigDecimal(10000000);
    // 触发大额交易的支付人和收款人限额
    private final BigDecimal payeeBeneficiaryLimit = new BigDecimal(20000000);

    /**
     * 构造函数，初始化基类TransactionsGenerator。
     * @param consumer 交易消费者，定义如何处理生成的交易数据。
     * @param maxRecordsPerSecond 每秒最大生成记录数。
     */
    public DemoTransactionsGenerator(Consumer<Transaction> consumer, int maxRecordsPerSecond) {
        super(consumer, maxRecordsPerSecond);
    }

    /**
     * 生成带有特定条件的随机交易事件。
     * @param rnd 随机数生成器，用于生成交易数据。
     * @return 返回增强特性的交易对象。
     */
    protected Transaction randomEvent(SplittableRandom rnd) {
        // 从基类生成一个普通交易事件
        Transaction transaction = super.randomEvent(rnd);
        // 获取当前时间
        long now = System.currentTimeMillis();

        // 检查是否需要触发大额交易（收款人）
        if (now - lastBeneficiaryIdTriggered > 8000 + rnd.nextInt(5000)) {
            // 设置交易金额为大额交易限额加随机值
            transaction.setPaymentAmount(beneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
            // 更新上次触发时间
            this.lastBeneficiaryIdTriggered = System.currentTimeMillis();
        }

        // 检查是否需要触发大额交易（支付人和收款人）
        if (now - lastPayeeIdBeneficiaryIdTriggered > 12000 + rnd.nextInt(10000)) {
            // 设置交易金额为大额交易限额加随机值
            transaction.setPaymentAmount(payeeBeneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
            // 更新上次触发时间
            this.lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
        }

        // 返回增强后的交易对象
        return transaction;
    }
}
