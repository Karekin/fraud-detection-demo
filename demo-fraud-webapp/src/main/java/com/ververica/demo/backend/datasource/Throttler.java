package com.ververica.demo.backend.datasource;

import com.google.common.base.Preconditions;

/**
 * 用于控制执行频率的工具类，限制线程每秒可以执行的操作次数。
 */
final class Throttler {

    private long throttleBatchSize; // 每个批次中的操作次数
    private long nanosPerBatch; // 每个批次所需的纳秒数

    private long endOfNextBatchNanos; // 下一个批次结束时间的纳秒时间戳
    private int currentBatch; // 当前批次中已执行的操作数

    /**
     * 构造函数，初始化Throttler的速率。
     * @param maxRecordsPerSecond 每秒允许的最大记录数。
     */
    Throttler(long maxRecordsPerSecond) {
        setup(maxRecordsPerSecond);
    }

    /**
     * 动态调整每秒允许的最大记录数。
     * @param maxRecordsPerSecond 新的每秒最大记录数。
     */
    public void adjustMaxRecordsPerSecond(long maxRecordsPerSecond) {
        setup(maxRecordsPerSecond);
    }

    /**
     * 根据每秒的记录数设定批次大小和每批次间隔。
     * @param maxRecordsPerSecond 每秒的记录数。
     */
    private synchronized void setup(long maxRecordsPerSecond) {
        Preconditions.checkArgument(
                maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");

        if (maxRecordsPerSecond == -1) {
            // 如果速率设置为无限制
            throttleBatchSize = -1;
            nanosPerBatch = 0;
            endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
            currentBatch = 0;
            return;
        }

        if (maxRecordsPerSecond >= 10000) {
            // 对于非常高的速率，以2毫秒为一个间隔进行限制
            throttleBatchSize = (int) maxRecordsPerSecond / 500;
            nanosPerBatch = 2_000_000L; // 2毫秒
        } else {
            // 对于较低的速率，计算适合的批次大小和时间间隔
            throttleBatchSize = ((int) (maxRecordsPerSecond / 20)) + 1;
            nanosPerBatch = ((int) (1_000_000_000L / maxRecordsPerSecond)) * throttleBatchSize;
        }
        this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
        this.currentBatch = 0;
    }

    /**
     * 使当前线程暂停，直到达到设定的执行频率。
     * @throws InterruptedException 如果线程在等待时被中断。
     */
    synchronized void throttle() throws InterruptedException {
        if (throttleBatchSize == -1) {
            // 如果没有限制，则直接返回
            return;
        }
        if (++currentBatch != throttleBatchSize) {
            // 如果当前批次还未完成，继续执行
            return;
        }
        // 完成一个批次，重置批次计数
        currentBatch = 0;

        // 计算需要等待的时间
        final long now = System.nanoTime();
        final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

        if (millisRemaining > 0) {
            // 如果还需等待，则暂停相应时间
            endOfNextBatchNanos += nanosPerBatch;
            Thread.sleep(millisRemaining);
        } else {
            // 如果超出等待时间，重新设定下一个批次的结束时间
            endOfNextBatchNanos = now + nanosPerBatch;
        }
    }
}
