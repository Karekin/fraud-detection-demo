package org.apache.flink.cep.coordinator;


import org.apache.flink.cep.discover.RuleDiscoverer;
import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.discover.RuleManager;
import org.apache.flink.cep.event.*;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.IOUtils.closeAll;

/**
 * 规则发现协调器
 *
 * 此类实现了 OperatorCoordinator 和 RuleManager 接口，
 * 用于协调规则的发现、分发以及管理等操作。
 *
 * @author shirukai
 */
public class RuleDistributorCoordinator implements OperatorCoordinator, RuleManager {
    private static final Logger LOG = LoggerFactory.getLogger(RuleDistributorCoordinator.class);

    // 与此 RuleDistributorCoordinator 相关联的算子名称
    private final String operatorName;

    // 用于创建规则发现器的工厂类
    private final RuleDiscovererFactory discovererFactory;

    // 协调器上下文对象，提供协调器运行时的执行环境和工具
    private final CoordinatorContext context;

    // 规则更新事件队列的标识符
    private final String ruleUpdatedQueueId;

    // 标志是否已启动协调器
    private boolean started;

    // 规则发现器实例，用于发现规则更新
    private RuleDiscoverer discoverer;

    // 当前规则绑定事件，用于维护规则与任务之间的绑定关系
    private RuleBindingEvent currentRuleBindingEvent;

    // 规则更新事件队列，用于存储规则更新事件并分发给其他组件
    private final LinkedBlockingQueue<RuleUpdatedEvent> updatedEventQueue;

    /**
     * 构造函数
     *
     * 初始化规则分发协调器所需的所有依赖，包括算子名称、队列ID、
     * 规则发现器工厂类以及协调器上下文。
     *
     * @param operatorName      算子的名称
     * @param ruleUpdatedQueueId 规则更新队列的ID
     * @param discovererFactory 规则发现器工厂类
     * @param coordinatorContext 协调器上下文对象
     */
    public RuleDistributorCoordinator(String operatorName,
                                      String ruleUpdatedQueueId,
                                      RuleDiscovererFactory discovererFactory,
                                      CoordinatorContext coordinatorContext) {
        this.operatorName = operatorName;
        this.discovererFactory = discovererFactory;
        this.context = coordinatorContext;
        this.ruleUpdatedQueueId = ruleUpdatedQueueId;

        // 初始化规则更新事件队列，通过上下文的存储机制获取或创建
        updatedEventQueue = getRuleUpdatedEventQueue();
    }

    /**
     * 启动规则发现器并初始化相关资源
     *
     * 该方法在协调器启动时调用，负责创建规则发现器并开始规则发现任务。
     * 如果发现器创建失败，则会终止作业并记录错误信息。
     *
     * @throws Exception 如果启动过程中出现问题
     */
    @Override
    public void start() throws Exception {
        LOG.info(
                "Starting RuleDiscoverer for {}: {}.",
                this.getClass().getSimpleName(),
                operatorName);

        // 标记协调器已启动，用于区分未启动和启动失败的情况
        started = true;

        // 如果规则发现器尚未创建，则尝试通过工厂创建发现器实例
        if (discoverer == null) {
            try {
                discoverer = discovererFactory.createRuleDiscoverer(context.getUserCodeClassloader());
            } catch (Throwable t) {
                // 如果创建过程中发生致命错误，重新抛出异常
                ExceptionUtils.rethrowIfFatalError(t);

                // 记录错误日志并终止作业
                LOG.error(
                        "Failed to create RuleDiscoverer for {}: {}.",
                        this.getClass().getSimpleName(),
                        operatorName,
                        t);
                context.failJob(t);
                return;
            }
        }

        // 将规则发现任务提交到协调器的事件循环中
        // 确保发现任务优先被执行，以保证后续操作的正确性
        runInEventLoop(
                () -> discoverer.discoverRuleUpdates(this),
                "discovering the Rule updates.");
    }

    /**
     * 关闭协调器并释放所有资源
     *
     * 该方法在协调器关闭时调用，负责关闭规则发现器以及清理相关资源。
     *
     * @throws Exception 如果关闭过程中出现问题
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing RuleDistributorCoordinator for rule distributor {}.", operatorName);

        // 如果协调器已启动，则关闭相关资源
        if (started) {
            closeAll(context, discoverer);
        }

        // 更新启动状态为未启动
        started = false;

        LOG.info("RuleDistributorCoordinator for rule distributor {} closed.", operatorName);
    }


    /**
     * 从算子接收事件的处理方法
     *
     * 当前实现为空方法（No-op），即不对从算子接收到的事件执行任何操作。
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
     * 协调器的检查点保存方法
     *
     * 该方法在检查点触发时调用，负责保存当前协调器的状态数据。
     *
     * @param checkpointId 检查点的唯一标识
     * @param resultFuture 异步保存状态数据的结果
     * @throws Exception 如果保存过程中发生错误
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        // 将检查点保存任务提交到事件循环中
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);

                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                         ObjectOutputStream out = new ObjectOutputStream(baos)) {

                        // 将当前规则绑定事件序列化并写入字节输出流
                        out.writeObject(currentRuleBindingEvent);
                        out.flush();

                        // 将序列化的字节数组设置为结果
                        resultFuture.complete(baos.toByteArray());

                    } catch (Throwable e) {
                        // 处理可能的致命错误或内存溢出错误
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);

                        // 如果发生异常，完成异常结果并记录错误日志
                        resultFuture.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint the RuleBindingEvent for rule distributor %s",
                                                operatorName),
                                        e));
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    /**
     * 通知检查点完成的方法
     *
     * 当某个检查点完成时调用，执行与检查点完成相关的逻辑。
     * 当前未实现具体逻辑。
     *
     * @param checkpointId 已完成的检查点ID
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 当前未定义具体的检查点完成处理逻辑
    }

    /**
     * 将协调器状态恢复到指定的检查点
     *
     * 该方法在作业恢复时调用，用于将协调器恢复到指定检查点的状态。
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
                "Restoring RuleDiscoverer of rule distributor {} from checkpoint.",
                operatorName);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
             ObjectInputStream in = new ObjectInputStream(bais)) {

            // 从检查点数据中读取规则绑定事件
            currentRuleBindingEvent = (RuleBindingEvent) in.readObject();
        }

        // 通过工厂创建新的规则发现器
        discoverer = discovererFactory.createRuleDiscoverer(context.getUserCodeClassloader());
    }


    /**
     * 重置指定子任务到某个检查点的状态
     *
     * 该方法在子任务需要恢复时调用，将检查点的状态数据发送到指定的子任务。
     *
     * @param subtask      子任务的索引
     * @param checkpointId 要恢复到的检查点ID
     */
    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.info(
                "Recovering subtask {} to checkpoint {} for rule distributor {} to checkpoint.",
                subtask,
                checkpointId,
                operatorName);

        // 将子任务状态恢复操作提交到事件循环中
        runInEventLoop(
                () -> {
                    // 如果当前存在规则绑定事件，则发送到指定子任务
                    if (currentRuleBindingEvent != null) {
                        context.sendEventToOperator(subtask, currentRuleBindingEvent);
                    }
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    /**
     * 处理子任务尝试失败的逻辑
     *
     * 该方法在子任务运行失败时调用，用于标记子任务未准备好并记录相关信息。
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
                            "Removing itself after failure for subtask {} of rule distributor {}.",
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
     * 当子任务准备好重新运行时调用，将子任务标记为已准备好并发送当前规则绑定事件。
     *
     * @param subtask       子任务的索引
     * @param attemptNumber 尝试编号
     * @param gateway       子任务的事件网关，用于发送事件
     */
    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        // 确保子任务索引与网关中的子任务匹配
        assert subtask == gateway.getSubtask();

        LOG.debug("Subtask {} of rule distributor {} is ready.", subtask, operatorName);

        // 将子任务标记为已准备好并发送规则绑定事件
        runInEventLoop(
                () -> {
                    // 标记子任务为已准备好
                    context.subtaskReady(gateway);

                    // 如果当前存在规则绑定事件，则发送到子任务
                    if (currentRuleBindingEvent != null) {
                        context.sendEventToOperator(subtask, currentRuleBindingEvent);
                    }
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    /**
     * 通知检查点中止
     *
     * 当某个检查点被中止时调用，用于记录检查点中止的日志。
     *
     * @param checkpointId 被中止的检查点ID
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as aborted for rule distributor {}.",
                checkpointId,
                operatorName);
    }


    /**
     * 确保协调器已启动
     *
     * 如果协调器尚未启动，则抛出 IllegalStateException 异常。
     * 该方法在需要执行关键操作时调用，以确保协调器处于正确的状态。
     */
    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    /**
     * 在事件循环中运行指定的操作
     *
     * 该方法将一个任务提交到协调器线程中执行，用于保证线程安全并
     * 确保所有任务在单线程环境下按顺序执行。
     *
     * @param action                    要执行的操作
     * @param actionName                操作的名称（用于记录日志）
     * @param actionNameFormatParameters 操作名称的格式化参数
     */
    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        // 确保协调器已启动
        ensureStarted();

        // 如果规则发现器尚未初始化（例如创建过程中失败），直接忽略任务
        if (discoverer == null) {
            return;
        }

        // 将任务提交到协调器线程
        context.runInCoordinatorThread(
                () -> {
                    try {
                        // 执行具体任务
                        action.run();
                    } catch (Throwable t) {
                        // 检测并重新抛出致命错误或内存溢出错误
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        // 构建操作名称的格式化字符串，用于日志记录
                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);

                        // 记录错误日志并触发作业故障恢复
                        LOG.error(
                                "Uncaught exception in the RuleDiscovererCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * 处理规则发现器查询到的规则更新
     *
     * 当规则发现器发现新的规则时调用该方法，
     * 将规则更新事件和规则绑定事件分发到相关组件。
     *
     * @param rules 查询到的规则列表
     */
    @Override
    public void onRuleUpdated(List<Rule> rules) {

        // 存储规则更新事件的列表
        List<RuleUpdated> updates = new ArrayList<>(rules.size());

        // 存储规则绑定事件的列表
        List<RuleBinding> bindings = new ArrayList<>(rules.size());

        // 遍历查询到的规则，分别创建更新事件和绑定事件
        for (Rule rule : rules) {
            updates.add(RuleUpdated.of(rule));
            bindings.add(RuleBinding.of(rule));
        }

        // 1. 生成规则更新事件，并通过队列发送到 RuleProcessorCoordinator
        try {
            updatedEventQueue.put(new RuleUpdatedEvent(updates));
        } catch (InterruptedException e) {
            // 如果队列操作失败，记录日志并终止作业
            LOG.error("Failed to send RuleUpdatedEvent to rule processor coordinator.", e);
            context.failJob(e);
            return;
        }

        // 2. 更新当前规则绑定事件
        currentRuleBindingEvent = new RuleBindingEvent(bindings);

        // 3. 将规则绑定事件发送到所有子任务
        for (int subtask : context.getSubtasks()) {
            try {
                context.sendEventToOperator(subtask, currentRuleBindingEvent);
            } catch (Exception e) {
                // 如果发送事件失败，记录日志并终止作业
                LOG.error(
                        "Failed to send RuleBindingEvent to rule distributor operator {}",
                        operatorName,
                        e);
                context.failJob(e);
                return;
            }
        }
    }

    /**
     * 获取或创建规则更新事件队列
     *
     * 该方法从协调器存储中获取规则更新事件队列，如果不存在则创建新队列。
     *
     * @return 用于存储规则更新事件的 LinkedBlockingQueue 实例
     */
    @SuppressWarnings("unchecked")
    public LinkedBlockingQueue<RuleUpdatedEvent> getRuleUpdatedEventQueue() {
        // 从协调器存储中获取现有队列或创建新队列
        CoordinatorStore coordinatorStore = context.getOperatorCoordinatorContext().getCoordinatorStore();
        return (LinkedBlockingQueue<RuleUpdatedEvent>) coordinatorStore.compute(
                ruleUpdatedQueueId,
                (key, value) -> {
                    if (value == null) {
                        // 如果存储中不存在队列，创建新队列
                        return new LinkedBlockingQueue<>();
                    } else {
                        // 如果队列已存在，返回现有队列
                        return value;
                    }
                }
        );
    }
}
