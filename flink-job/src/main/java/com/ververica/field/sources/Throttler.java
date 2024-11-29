package com.ververica.field.sources;

import org.apache.flink.util.Preconditions;

/**
 * Throttler类用于限制线程每秒执行的记录数，确保每个并行子任务不会超过指定的速率。
 * 该类会根据每秒记录数和并行子任务数来计算每个批次需要等待的时间，从而控制执行速率。
 */
final class Throttler {

    private final long throttleBatchSize; // 每个批次的大小（即每个批次需要处理的记录数）
    private final long nanosPerBatch;    // 每个批次之间的等待时间（以纳秒为单位）

    private long endOfNextBatchNanos;    // 下一个批次的结束时间（以纳秒为单位）
    private int currentBatch;            // 当前批次中的记录数

    /**
     * 构造函数，初始化 Throttler，计算每秒执行的记录数和每个批次的等待时间。
     *
     * @param maxRecordsPerSecond      最大每秒记录数，如果为-1表示无限制
     * @param numberOfParallelSubtasks 并行子任务的数量
     */
    Throttler(long maxRecordsPerSecond, int numberOfParallelSubtasks) {
        // 检查 maxRecordsPerSecond 是否为负数，且必须大于零或为-1
        Preconditions.checkArgument(
                maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");

        // 检查并行子任务数是否大于0
        Preconditions.checkArgument(
                numberOfParallelSubtasks > 0, "numberOfParallelSubtasks must be greater than 0");

        if (maxRecordsPerSecond == -1) {
            // 如果 maxRecordsPerSecond 为 -1，表示没有速率限制
            throttleBatchSize = -1;      // 批次大小设置为-1，表示没有限制
            nanosPerBatch = 0;           // 不需要等待时间
            endOfNextBatchNanos = System.nanoTime() + nanosPerBatch; // 设置下一个批次的结束时间
            currentBatch = 0;            // 当前批次记录数初始化为 0
            return;
        }

        // 计算每个并行子任务的速率
        final float ratePerSubtask = (float) maxRecordsPerSecond / numberOfParallelSubtasks;

        if (ratePerSubtask >= 10000) {
            // 对于高速率（每秒记录数大于等于 10000），每批次的大小按 2 毫秒控制
            throttleBatchSize = (int) ratePerSubtask / 500; // 每个批次处理的记录数
            nanosPerBatch = 2_000_000L; // 每个批次等待 2 毫秒（2 毫秒 = 2,000,000 纳秒）
        } else {
            // 对于低速率（每秒记录数小于 10000），按 1 秒内能处理的记录数来计算批次大小
            throttleBatchSize = ((int) (ratePerSubtask / 20)) + 1; // 每个批次处理的记录数
            nanosPerBatch = ((int) (1_000_000_000L / ratePerSubtask)) * throttleBatchSize; // 计算每个批次的等待时间
        }

        // 设置下一个批次的结束时间
        this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
        this.currentBatch = 0; // 当前批次初始化为 0
    }

    /**
     * 控制线程速率，确保不超过最大每秒记录数。
     * 如果当前批次的记录数达到设定的批次大小，就会进行等待，直到达到下一个批次的时间。
     *
     * @throws InterruptedException 如果线程在等待期间被中断
     */
    void throttle() throws InterruptedException {
        // 如果没有速率限制（throttleBatchSize 为 -1），直接返回
        if (throttleBatchSize == -1) {
            return;
        }

        // 当前批次记录数加 1，如果还没有达到批次大小，则返回
        if (++currentBatch != throttleBatchSize) {
            return;
        }

        // 达到批次大小时，重置当前批次记录数，并计算等待时间
        currentBatch = 0;

        // 获取当前时间（以纳秒为单位）
        final long now = System.nanoTime();

        // 计算当前批次结束前还剩余的时间（单位：毫秒）
        final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

        // 如果还剩余时间，需要等待一段时间
        if (millisRemaining > 0) {
            endOfNextBatchNanos += nanosPerBatch; // 更新下一个批次的结束时间
            Thread.sleep(millisRemaining); // 等待剩余的毫秒数
        } else {
            // 如果没有剩余时间，直接更新下一个批次的结束时间
            endOfNextBatchNanos = now + nanosPerBatch;
        }
    }
}
