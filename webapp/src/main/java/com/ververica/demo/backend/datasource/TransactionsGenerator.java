package com.ververica.demo.backend.datasource;

import com.ververica.demo.backend.datasource.Transaction.PaymentType;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

/**
 * 交易数据生成器
 * 用于持续生成模拟的交易数据，可配置生成速率。
 */
@Slf4j
public class TransactionsGenerator implements Runnable {

    // 支付人ID的最大值
    private static long MAX_PAYEE_ID = 100000;
    // 受益人ID的最大值
    private static long MAX_BENEFICIARY_ID = 100000;

    // 最小交易金额
    private static double MIN_PAYMENT_AMOUNT = 5d;
    // 最大交易金额
    private static double MAX_PAYMENT_AMOUNT = 20d;
    // 用于控制数据生成速率的节流器
    private final Throttler throttler;

    // 控制生成循环是否继续运行的标志
    private volatile boolean running = true;
    // 每秒生成的最大记录数
    private Integer maxRecordsPerSecond;

    // 处理每条生成交易的消费者
    private final Consumer<Transaction> consumer;

    /**
     * 构造函数
     * @param consumer 交易数据的消费者，定义如何处理每一条生成的交易数据。
     * @param maxRecordsPerSecond 每秒最大生成的交易记录数。
     */
    public TransactionsGenerator(Consumer<Transaction> consumer, int maxRecordsPerSecond) {
        this.consumer = consumer;
        this.maxRecordsPerSecond = maxRecordsPerSecond;
        this.throttler = new Throttler(maxRecordsPerSecond);
    }

    /**
     * 动态调整每秒最大生成记录数
     * @param maxRecordsPerSecond 新的每秒最大记录数
     */
    public void adjustMaxRecordsPerSecond(long maxRecordsPerSecond) {
        throttler.adjustMaxRecordsPerSecond(maxRecordsPerSecond);
    }

    /**
     * 生成一个随机的交易事件。
     * 此方法利用传入的随机数生成器来生成具有随机特性的交易对象。
     *
     * @param rnd 随机数生成器，用于生成随机交易ID、收款人ID、受益人ID和交易金额。
     * @return 生成的交易对象，包含交易ID、收款人ID、受益人ID、交易金额、交易类型和事件时间戳。
     */
    protected Transaction randomEvent(SplittableRandom rnd) {
        // 生成随机的交易ID，范围从0到Long.MAX_VALUE
        long transactionId = rnd.nextLong(Long.MAX_VALUE);
        // 生成随机的支付人ID，范围从0到MAX_PAYEE_ID（100000）
        long payeeId = rnd.nextLong(MAX_PAYEE_ID);
        // 生成随机的受益人ID，范围从0到MAX_BENEFICIARY_ID（100000）
        long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
        // 生成随机的交易金额，范围从MIN_PAYMENT_AMOUNT（5）到MAX_PAYMENT_AMOUNT（20）
        double paymentAmountDouble =
                ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
        // 将交易金额四舍五入到小数点后两位
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
        // 创建BigDecimal对象，用于精确表示交易金额
        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);

        // 构建并返回交易对象
        return Transaction.builder()
                .transactionId(transactionId) // 设置交易ID
                .payeeId(payeeId) // 设置支付人ID
                .beneficiaryId(beneficiaryId) // 设置受益人ID
                .paymentAmount(paymentAmount) // 设置交易金额
                .paymentType(paymentType(transactionId)) // 设置交易类型，根据ID的奇偶性决定
                .eventTime(System.currentTimeMillis()) // 设置事件生成的时间戳
                .build();
    }


    /**
     * 生成一条交易数据
     * @return 生成的交易对象
     */
    public Transaction generateOne() {
        return randomEvent(new SplittableRandom());
    }

    /**
     * 根据交易ID决定交易类型
     * @param id 交易ID
     * @return 交易类型
     */
    private static PaymentType paymentType(long id) {
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return PaymentType.CRD; // 信用卡支付
            case 1:
                return PaymentType.CSH; // 现金支付
            default:
                throw new IllegalStateException("Unexpected payment type");
        }
    }

    /**
     * 作为线程运行，不断生成交易数据
     */
    @Override
    public final void run() {
        running = true;
        final SplittableRandom rnd = new SplittableRandom();

        while (running) {
            Transaction event = randomEvent(rnd);
            log.debug("{}", event);
            consumer.accept(event);
            try {
                throttler.throttle();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        log.info("Finished run()");
    }

    /**
     * 停止生成交易数据
     */
    public final void cancel() {
        running = false;
        log.info("Cancelled");
    }
}
