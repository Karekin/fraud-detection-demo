package org.apache.flink.cep.coordinator;


import org.apache.flink.cep.event.RuleUpdatedEvent;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.IOUtils.closeAll;

/**
 * 规则处理协调器
 *
 * <p>该类实现了 {@link OperatorCoordinator} 接口，
 * 用于在 Flink 中管理规则处理的协调逻辑。它负责：
 * <ul>
 *   <li>启动规则更新消费者以处理规则更新事件。
 *   <li>管理子任务的状态，包括准备好和未准备好的状态切换。
 *   <li>实现检查点保存与恢复，确保规则处理的状态一致性。
 * </ul>
 *
 * @author shirukai
 */
public class RuleProcessorCoordinator implements OperatorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(RuleProcessorCoordinator.class);

    // 算子名称，用于标识该协调器关联的算子
    private final String operatorName;

    // 协调器上下文，提供与 Flink 框架交互的工具
    private final CoordinatorContext context;

    // 标记协调器是否已启动
    private boolean started;

    // 规则更新队列的唯一标识符
    private final String ruleUpdatedQueueId;

    // 当前正在处理的规则更新事件
    private RuleUpdatedEvent currentRuleUpdatedEvent;

    // 存储规则更新事件的阻塞队列
    private final LinkedBlockingQueue<RuleUpdatedEvent> updatedEventQueue;

    /**
     * 构造方法
     *
     * <p>初始化规则处理协调器实例，包括算子名称、规则更新队列标识符、
     * 以及协调器上下文。
     *
     * @param operatorName 算子的名称
     * @param ruleUpdatedQueueId 规则更新队列的标识符
     * @param coordinatorContext 协调器上下文对象
     */
    public RuleProcessorCoordinator(
            String operatorName,
            String ruleUpdatedQueueId,
            CoordinatorContext coordinatorContext) {
        this.operatorName = operatorName;
        this.context = coordinatorContext;
        this.ruleUpdatedQueueId = ruleUpdatedQueueId;

        // 从协调器存储中获取规则更新事件队列
        this.updatedEventQueue = getRuleUpdatedEventQueue();
    }


    /**
     * 启动协调器
     *
     * <p>启动规则更新消费者线程，持续消费规则更新事件并将其分发到子任务。
     *
     * @throws Exception 如果启动过程中发生错误
     */
    @Override
    public void start() throws Exception {
        LOG.info(
                "Starting RuleUpdatedConsumer for {}: {}.",
                this.getClass().getSimpleName(),
                operatorName);

        // 标记协调器已启动，用于区分未启动和启动失败的情况
        started = true;

        // 将规则更新消费任务提交到协调器线程的事件循环中
        runInEventLoop(
                this::consumeRuleUpdates,
                "consuming the Rule updates.");
    }

    /**
     * 关闭协调器
     *
     * <p>释放协调器的所有资源，并停止规则更新消费者线程。
     *
     * @throws Exception 如果关闭过程中发生错误
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing RuleProcessorCoordinator for rule processor {}.", operatorName);

        // 如果协调器已启动，则关闭相关资源
        if (started) {
            closeAll(context);
        }

        // 标记为未启动
        started = false;

        LOG.info("RuleProcessorCoordinator for rule processor {} closed.", operatorName);
    }


    /**
     * 从算子接收事件的处理方法
     *
     * <p>当算子发送事件到协调器时调用。
     * 当前实现为空操作（No-op），即未处理接收到的事件。
     *
     * @param subtask       发送事件的子任务索引
     * @param attemptNumber 子任务的尝试编号
     * @param event         接收到的事件对象
     * @throws Exception 如果事件处理失败
     */
    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        // 当前未定义具体的事件处理逻辑
    }

    /**
     * 保存协调器的检查点
     *
     * <p>该方法在检查点触发时调用，负责保存当前协调器的状态数据。
     *
     * @param checkpointId 检查点的唯一标识
     * @param resultFuture 异步保存状态数据的结果
     * @throws Exception 如果保存过程中发生错误
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);

                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                         ObjectOutputStream out = new ObjectOutputStream(baos)) {
                        // 序列化当前规则更新事件并写入字节流
                        out.writeObject(currentRuleUpdatedEvent);
                        out.flush();

                        // 将序列化的字节数组设置为结果
                        resultFuture.complete(baos.toByteArray());

                    } catch (Throwable e) {
                        // 捕获致命错误或内存溢出错误
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);

                        // 如果发生异常，完成异常结果并记录错误日志
                        resultFuture.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint the RuleUpdatedEvent for rule distributor %s",
                                                operatorName),
                                        e));
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    /**
     * 通知检查点完成
     *
     * <p>当某个检查点完成时调用。当前未实现具体逻辑。
     *
     * @param checkpointId 已完成的检查点ID
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 当前未定义具体的检查点完成处理逻辑
    }

    /**
     * 恢复协调器到指定的检查点状态
     *
     * <p>该方法在作业恢复时调用，用于将协调器恢复到指定检查点的状态。
     * 如果检查点数据为空，则不进行恢复。
     *
     * @param checkpointId   检查点的唯一标识
     * @param checkpointData 检查点的状态数据
     * @throws Exception 如果恢复过程中发生错误
     */
    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        // 如果检查点数据为空，则跳过恢复
        if (checkpointData == null) {
            return;
        }

        LOG.info(
                "Restoring RuleUpdatedEvent of rule processor {} from checkpoint.",
                operatorName);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
             ObjectInputStream in = new ObjectInputStream(bais)) {
            // 从检查点数据中读取规则更新事件
            currentRuleUpdatedEvent = (RuleUpdatedEvent) in.readObject();
        }
    }


    /**
     * 重置指定子任务到某个检查点的状态
     *
     * <p>该方法在子任务需要恢复时调用，将检查点的状态数据发送到指定的子任务。
     *
     * @param subtask      子任务的索引
     * @param checkpointId 要恢复到的检查点ID
     */
    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.info(
                "Recovering subtask {} to checkpoint {} for rule processor {} to checkpoint.",
                subtask,
                checkpointId,
                operatorName);

        // 将子任务状态恢复操作提交到协调器线程的事件循环中
        runInEventLoop(
                () -> {
                    // 如果当前存在规则更新事件，则发送到指定子任务
                    if (currentRuleUpdatedEvent != null) {
                        context.sendEventToOperator(subtask, currentRuleUpdatedEvent);
                    }
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    /**
     * 处理子任务尝试失败的逻辑
     *
     * <p>该方法在子任务运行失败时调用，用于标记子任务未准备好并记录相关信息。
     *
     * @param subtask       发生失败的子任务索引
     * @param attemptNumber 失败尝试的编号
     * @param reason        失败的原因（可以为空）
     */
    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Removing itself after failure for subtask {} of rule processor {}.",
                            subtask,
                            operatorName);

                    // 将子任务标记为未准备好
                    context.subtaskNotReady(subtask);
                },
                "handling subtask %d failure",
                subtask);
    }

    /**
     * 标记子任务尝试为准备好
     *
     * <p>当子任务准备好重新运行时调用，将子任务标记为已准备好并发送当前规则更新事件。
     *
     * @param subtask       子任务的索引
     * @param attemptNumber 尝试编号
     * @param gateway       子任务的事件网关，用于发送事件
     */
    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        // 确保子任务索引与网关中的子任务匹配
        assert subtask == gateway.getSubtask();

        LOG.debug("Subtask {} of rule processor {} is ready.", subtask, operatorName);

        // 将子任务标记为已准备好并发送规则更新事件
        runInEventLoop(
                () -> {
                    // 标记子任务为已准备好
                    context.subtaskReady(gateway);

                    // 如果当前存在规则更新事件，则发送到子任务
                    if (currentRuleUpdatedEvent != null) {
                        context.sendEventToOperator(subtask, currentRuleUpdatedEvent);
                    }
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    /**
     * 通知检查点中止
     *
     * <p>当某个检查点被中止时调用，用于记录检查点中止的日志。
     *
     * @param checkpointId 被中止的检查点ID
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as aborted for rule processor {}.",
                checkpointId,
                operatorName);
    }


    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    /**
     * 在协调器线程中运行指定的任务
     *
     * <p>该方法将一个任务提交到协调器的线程中执行，确保任务在线程安全的环境中
     * 按顺序执行。如果任务在执行过程中抛出异常，则会记录日志并触发作业故障恢复。
     *
     * @param action 要执行的任务，定义为一个可抛出异常的操作
     * @param actionName 任务的名称（用于记录日志）
     * @param actionNameFormatParameters 任务名称的格式化参数
     */
    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        // 确保协调器已启动，否则抛出异常
        ensureStarted();

        // 将任务提交到协调器线程池中
        context.runInCoordinatorThread(
                () -> {
                    try {
                        // 执行指定任务
                        action.run();
                    } catch (Throwable t) {
                        // 捕获致命错误或内存溢出错误，立即重新抛出
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        // 格式化任务名称以记录日志
                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);

                        // 记录错误日志并触发作业故障恢复
                        LOG.error(
                                "Uncaught exception in the RuleProcessorCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * 消费规则更新事件
     *
     * <p>该方法通过一个守护线程持续从规则更新队列中取出事件，并将其分发到所有子任务。
     * 如果事件处理失败，将记录错误日志并触发作业失败。
     *
     * @throws InterruptedException 如果线程被中断
     */
    public void consumeRuleUpdates() throws InterruptedException {
        // 创建并启动规则更新消费者线程
        Thread consumerThread = new Thread(() -> {
            while (started) { // 检查协调器是否已启动
                try {
                    // 从规则更新队列中取出事件（阻塞操作）
                    currentRuleUpdatedEvent = updatedEventQueue.take();

                    // 将规则更新事件发送到所有子任务
                    for (int subtask : context.getSubtasks()) {
                        context.sendEventToOperator(subtask, currentRuleUpdatedEvent);
                    }
                } catch (Exception e) {
                    // 捕获事件处理失败的异常并记录错误日志
                    LOG.error(
                            "Failed to send RuleUpdatedEvent to rule processor operator {}",
                            operatorName,
                            e);

                    // 触发作业失败
                    context.failJob(e);
                    return;
                }
            }
        });

        // 设置消费者线程为守护线程，确保在 JVM 退出时自动停止
        consumerThread.setDaemon(true);
        consumerThread.start();
    }


    @SuppressWarnings("unchecked")
    /**
     * 获取规则更新事件队列
     *
     * <p>该方法从协调器存储中检索规则更新事件队列。如果队列不存在，
     * 则创建一个新的 {@link LinkedBlockingQueue} 实例并返回。
     *
     * @return 用于存储规则更新事件的 {@link LinkedBlockingQueue}
     */
    public LinkedBlockingQueue<RuleUpdatedEvent> getRuleUpdatedEventQueue() {
        // 获取协调器存储对象
        CoordinatorStore coordinatorStore = context.getOperatorCoordinatorContext().getCoordinatorStore();

        // 从存储中获取队列，如果不存在则创建新队列
        return (LinkedBlockingQueue<RuleUpdatedEvent>) coordinatorStore.compute(
                ruleUpdatedQueueId,
                (key, value) -> {
                    if (value == null) {
                        // 如果存储中不存在队列，创建一个新的阻塞队列
                        return new LinkedBlockingQueue<>();
                    } else {
                        // 如果队列已存在，直接返回
                        return value;
                    }
                }
        );
    }

}
