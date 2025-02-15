/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.coordinator;


import lombok.Getter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.coordination.ComponentClosingUtils;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link OperatorCoordinator} 的上下文类。
 *
 * <p>此上下文类的主要用途包括：
 *
 * <ul>
 *   <li>线程模型的强制执行 - 确保所有对协调器状态的操作均由同一线程处理。
 *   <li>提供对算子协调器的访问和任务网关的管理。
 * </ul>
 *
 * <p>此类实现了 {@link AutoCloseable} 接口，因此可以通过 try-with-resources 语句进行自动资源管理。
 */
@Internal
public class CoordinatorContext implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorContext.class);

    // 用于运行协调器任务的单线程调度执行器
    private final ScheduledExecutorService coordinatorExecutor;

    // 用于运行工作线程的单线程调度执行器
    private final ScheduledExecutorService workerExecutor;

    // 用于确保协调器任务在线程安全的环境中运行的线程工厂
    private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;

    @Getter
    // 算子协调器的上下文，用于与 Flink 框架交互
    private final OperatorCoordinator.Context operatorCoordinatorContext;

    // 存储所有子任务的网关，以支持与子任务的通信
    private final Map<Integer, OperatorCoordinator.SubtaskGateway> subtaskGateways;

    /**
     * 构造函数
     *
     * 使用协调器线程工厂和算子协调器上下文创建协调器上下文实例。
     *
     * @param coordinatorThreadFactory 用于创建协调器线程的线程工厂
     * @param operatorCoordinatorContext 算子协调器的上下文
     */
    public CoordinatorContext(
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this(
                Executors.newScheduledThreadPool(1, coordinatorThreadFactory), // 初始化协调器执行器
                Executors.newScheduledThreadPool(
                        1,
                        new ExecutorThreadFactory(
                                coordinatorThreadFactory.getCoordinatorThreadName() + "-worker")), // 初始化工作执行器
                coordinatorThreadFactory,
                operatorCoordinatorContext);
    }

    /**
     * 完整构造函数
     *
     * 使用指定的调度执行器、线程工厂和算子协调器上下文创建实例。
     *
     * @param coordinatorExecutor 协调器任务的调度执行器
     * @param workerExecutor 工作任务的调度执行器
     * @param coordinatorThreadFactory 协调器线程工厂
     * @param operatorCoordinatorContext 算子协调器上下文
     */
    public CoordinatorContext(
            ScheduledExecutorService coordinatorExecutor,
            ScheduledExecutorService workerExecutor,
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this.coordinatorExecutor = coordinatorExecutor;
        this.workerExecutor = workerExecutor;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;

        // 初始化子任务网关映射，大小为当前任务的并行度
        this.subtaskGateways = new HashMap<>(operatorCoordinatorContext.currentParallelism());
    }

    /**
     * 关闭协调器上下文
     *
     * 该方法会强制关闭工作线程池和协调器线程池，以确保资源被释放。
     * 采用静默关闭方式，即使关闭过程中出现异常，也会继续执行剩余的关闭操作。
     *
     * @throws InterruptedException 如果线程池关闭被中断
     */
    @Override
    public void close() throws InterruptedException {
        // 静默关闭工作线程池
        ComponentClosingUtils.shutdownExecutorForcefully(
                workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        // 静默关闭协调器线程池
        ComponentClosingUtils.shutdownExecutorForcefully(
                coordinatorExecutor, Duration.ofNanos(Long.MAX_VALUE));
    }

    /**
     * 在协调器线程中运行指定的任务
     *
     * 该方法将一个任务提交到协调器的线程中运行，确保任务在协调器的线程环境中
     * 按顺序执行，从而避免线程安全问题。
     *
     * @param runnable 要在协调器线程中执行的任务
     */
    public void runInCoordinatorThread(Runnable runnable) {
        // 将任务提交到协调器线程池的任务队列中
        coordinatorExecutor.execute(runnable);
    }


    // --------- Package private methods for the DynamicCepOperatorCoordinator ------------
    /**
     * 获取用户代码的类加载器
     *
     * 该方法返回当前任务的用户代码类加载器，用于加载用户提供的类。
     *
     * @return 用户代码的类加载器
     */
    ClassLoader getUserCodeClassloader() {
        return this.operatorCoordinatorContext.getUserCodeClassloader();
    }

    /**
     * 标记子任务为已准备好
     *
     * 当一个子任务准备好接收事件时调用，将子任务的网关存储在子任务网关映射中。
     * 如果子任务网关已存在，则抛出 IllegalStateException。
     *
     * @param gateway 子任务网关对象
     */
    void subtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        final int subtask = gateway.getSubtask();
        // 检查子任务是否已存在对应网关
        if (subtaskGateways.get(subtask) == null) {
            // 存储子任务的网关
            subtaskGateways.put(subtask, gateway);
        } else {
            throw new IllegalStateException("Already have a subtask gateway for " + subtask);
        }
    }

    /**
     * 标记子任务为未准备好
     *
     * 当一个子任务因失败或其他原因无法接收事件时调用，
     * 将该子任务的网关从映射中移除（设置为 null）。
     *
     * @param subtaskIndex 子任务的索引
     */
    void subtaskNotReady(int subtaskIndex) {
        // 将子任务的网关设置为 null 表示未准备好
        subtaskGateways.put(subtaskIndex, null);
    }

    /**
     * 获取所有子任务的索引
     *
     * 返回当前所有子任务的索引集合，用于遍历和操作子任务。
     *
     * @return 包含所有子任务索引的集合
     */
    Set<Integer> getSubtasks() {
        return subtaskGateways.keySet();
    }

    /**
     * 向子任务发送事件
     *
     * 该方法在协调器线程中调用，将指定事件发送到目标子任务的网关。
     * 如果子任务尚未准备好接收事件，会记录警告日志。
     *
     * @param subtaskId 目标子任务的索引
     * @param event     要发送的事件
     */
    public void sendEventToOperator(int subtaskId, OperatorEvent event) {
        // 在协调器线程中执行事件发送逻辑
        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway = subtaskGateways.get(subtaskId);
                    if (gateway == null) {
                        // 如果子任务未准备好，记录警告日志
                        LOG.warn(
                                String.format(
                                        "Subtask %d is not ready yet to receive events.",
                                        subtaskId));
                    } else {
                        // 通过网关发送事件到子任务
                        gateway.sendEvent(event);
                    }
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }


    /**
     * 使作业失败
     *
     * 调用此方法会通过协调器上下文报告作业失败，并将失败原因记录下来。
     *
     * @param cause 作业失败的原因
     */
    void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause);
    }

    // ---------------- private helper methods -----------------

    /**
     * 在协调器线程中执行指定任务
     *
     * 该方法用于确保任务在协调器线程中执行。如果当前线程不是协调器线程，
     * 则将任务提交到协调器线程池，并阻塞当前线程直到任务完成。
     * 如果当前线程已经是协调器线程，则直接执行任务。
     *
     * @param callable     要执行的任务（返回值由任务确定）
     * @param errorMessage 当任务失败时的错误信息
     * @param <V>          返回值的类型
     * @return 任务执行后的返回值
     * @throws FlinkRuntimeException 如果任务执行失败
     */
    private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // 如果当前线程不是协调器线程并且线程池未关闭
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()
                && !coordinatorExecutor.isShutdown()) {
            try {
                // 包装任务以捕获未处理的异常
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                // 执行任务并返回结果
                                return callable.call();
                            } catch (Throwable t) {
                                // 捕获异常并记录错误日志
                                LOG.error("Uncaught Exception in Coordinator Executor", t);
                                ExceptionUtils.rethrowException(t); // 重新抛出异常
                                return null; // 此处不会到达
                            }
                        };

                // 将任务提交到协调器线程池，并等待任务完成
                return coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                // 如果任务执行失败，抛出带有错误信息的运行时异常
                throw new FlinkRuntimeException(errorMessage, e);
            }
        }

        try {
            // 当前线程已是协调器线程，直接执行任务
            return callable.call();
        } catch (Throwable t) {
            // 捕获异常并记录错误日志
            LOG.error("Uncaught Exception in Source Coordinator Executor", t);
            throw new FlinkRuntimeException(errorMessage, t); // 抛出异常
        }
    }
}
