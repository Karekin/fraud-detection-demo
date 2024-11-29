package com.ververica.field.sources;

import static org.apache.flink.util.Preconditions.checkArgument;

import java.util.SplittableRandom;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 一个简单的随机数据生成器，带有数据速率限制逻辑。
 */
public abstract class BaseGenerator<T> extends RichParallelSourceFunction<T>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    // 每秒最大生成记录数，-1 表示无限制
    protected int maxRecordsPerSecond;

    // 控制数据生成是否继续的标志
    private volatile boolean running = true;

    // 当前子任务的ID
    private long id = -1;

    // 用于保存 ID 的状态，支持检查点
    private transient ListState<Long> idState;

    // 无参构造函数，默认最大记录数为无限制
    protected BaseGenerator() {
        this.maxRecordsPerSecond = -1;
    }

    // 带参构造函数，用于设置最大记录数
    protected BaseGenerator(int maxRecordsPerSecond) {
        // 校验最大记录数必须大于0或者为-1（表示无限制）
        checkArgument(
                maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");
        this.maxRecordsPerSecond = maxRecordsPerSecond;
    }

    // 打开资源，初始化 ID
    @Override
    public void open(Configuration parameters) throws Exception {
        // 如果 id 还未初始化，则根据子任务索引来设置 id
        if (id == -1) {
            id = getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    // 数据生成的核心逻辑
    @Override
    public final void run(SourceContext<T> ctx) throws Exception {
        // 获取并行子任务的数量
        final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        // 创建流量控制器（速率限制器）
        final Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks);
        // 随机数生成器
        final SplittableRandom rnd = new SplittableRandom();

        // 获取用于检查点的锁
        final Object lock = ctx.getCheckpointLock();

        // 持续生成数据直到 cancel 被触发
        while (running) {
            // 根据随机数生成事件
            T event = randomEvent(rnd, id);

            // 使用锁确保数据收集时线程安全
            synchronized (lock) {
                ctx.collect(event); // 收集生成的数据
                id += numberOfParallelSubtasks; // 更新 id，使得不同子任务生成不同的 ID
            }

            // 控制生成速率，避免超出最大记录数限制
            throttler.throttle();
        }
    }

    // 取消数据生成
    @Override
    public final void cancel() {
        running = false;
    }

    // 快照当前状态，保存 id 状态
    @Override
    public final void snapshotState(FunctionSnapshotContext context) throws Exception {
        idState.clear(); // 清空当前的状态
        idState.add(id); // 保存当前 id
    }

    // 初始化状态，恢复检查点数据
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 获取保存 id 状态的 ListState
        idState =
                context
                        .getOperatorStateStore()
                        .getUnionListState(new ListStateDescriptor<>("ids", BasicTypeInfo.LONG_TYPE_INFO));

        // 如果是恢复状态，恢复 id
        if (context.isRestored()) {
            long max = Long.MIN_VALUE;
            // 遍历所有的历史 id，选择最大的 id 值
            for (Long value : idState.get()) {
                max = Math.max(max, value);
            }

            // 恢复后的 id = 最大 id + 当前子任务的索引
            id = max + getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    // 抽象方法，由子类实现随机事件生成逻辑
    public abstract T randomEvent(SplittableRandom rnd, long id);

    // 获取最大记录数
    public int getMaxRecordsPerSecond() {
        return maxRecordsPerSecond;
    }
}
